use libc::{c_uint};
use std::ptr;

use ffi::{self};

use crate::core::{ MdbError, MdbResult, StateError };
use crate::database::{ Database, DbHandle};
use crate::environment::{ Environment };


#[derive(Copy, PartialEq, Debug, Eq, Clone)]
pub enum TransactionState {
    Normal,   // Normal, any operation possible
    Released, // Released (reset on readonly), has to be renewed
    Invalid,  // Invalid, no further operation possible
}

#[derive(Debug, Clone)]
pub struct NativeTransaction<'a> {
    pub handle: *mut ffi::MDB_txn,
    pub env: &'a Environment,
    flags: usize,
    pub state: TransactionState,
}

impl<'a> NativeTransaction<'a> {
    pub fn new_with_handle(h: *mut ffi::MDB_txn, flags: usize, env: &Environment) -> NativeTransaction {
        // debug!("new native txn");
        NativeTransaction {
            handle: h,
            flags,
            state: TransactionState::Normal,
            env,
        }
    }

    fn is_readonly(&self) -> bool {
        (self.flags as u32 & ffi::MDB_RDONLY) == ffi::MDB_RDONLY
    }

    pub fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        // debug!("commit txn");
        self.state = if self.is_readonly() {
            TransactionState::Released
        } else {
            TransactionState::Invalid
        };
        try_mdb!(unsafe { ffi::mdb_txn_commit(self.handle) } );
        Ok(())
    }

    fn abort(&mut self) {
        if self.state != TransactionState::Normal {
            debug!("Can't abort transaction: current state {:?}", self.state)
        } else {
            // debug!("abort txn");
            unsafe { ffi::mdb_txn_abort(self.handle); }
            self.state = if self.is_readonly() {
                TransactionState::Released
            } else {
                TransactionState::Invalid
            };
        }
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by a call to `renew`
    fn reset(&mut self) {
        if self.state != TransactionState::Normal {
            debug!("Can't reset transaction: current state {:?}", self.state);
        } else {
            unsafe { ffi::mdb_txn_reset(self.handle); }
            self.state = TransactionState::Released;
        }
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Released);
        try_mdb!(unsafe {ffi::mdb_txn_renew(self.handle)});
        self.state = TransactionState::Normal;
        Ok(())
    }

    fn new_child(&self, flags: c_uint) -> MdbResult<NativeTransaction> {
        let mut out: *mut ffi::MDB_txn = ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_txn_begin(ffi::mdb_txn_env(self.handle), self.handle, flags, &mut out) });
        Ok(NativeTransaction::new_with_handle(out, flags as usize, self.env))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        if self.state == TransactionState::Normal {
            // debug!("silent abort");
            unsafe {ffi::mdb_txn_abort(self.handle);}
            self.state = TransactionState::Invalid;
        }
    }

}

impl<'a> Drop for NativeTransaction<'a> {
    fn drop(&mut self) {
        //debug!("Dropping native transaction!");
        self.silent_abort();
    }
}

pub trait Txn<'a>: std::fmt::Debug {
    // fn get_inner_txn<'b>(&'a self) -> &'a NativeTransaction<'a>;
    fn get_handle(&self) -> *mut ffi::MDB_txn;
    fn get_env(&self) -> &'a Environment;
    fn get_state(&self) -> TransactionState;
}

#[derive(Debug, Clone)]
pub struct Transaction<'a> {
    inner: NativeTransaction<'a>,
}

impl<'a> Txn<'a> for Transaction<'a> {
    // fn get_inner_txn<'b>(&'a self) -> &'a NativeTransaction<'a> {
    //     &self.inner
    // }
    fn get_handle(&self) -> *mut ffi::MDB_txn {
        self.inner.handle
    }
    fn get_env(&self) -> &'a Environment {
        self.inner.env
    }
    fn get_state(&self) -> TransactionState {
        self.inner.state
    }
}

impl<'a> Transaction<'a> {
    pub fn new_with_native(txn: NativeTransaction<'a>) -> Transaction<'a> {
        Transaction {
            inner: txn
        }
    }

    pub fn new_child(&self) -> MdbResult<Transaction> {
        self.inner.new_child(0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(ffi::MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    /// Commits transaction, moves it out
    pub fn commit(self) -> MdbResult<()> {
        //self.inner.commit()
        let mut t = self;
        t.inner.commit()
    }

    /// Aborts transaction, moves it out
    pub fn abort(self) {
        let mut t = self;
        t.inner.abort();
    }

    // pub fn bind(&self, db_handle: &DbHandle) -> Database {
    //     Database::new_with_handle(db_handle.handle)
    // }
}


#[derive(Debug, Clone)]
pub struct ReadonlyTransaction<'a> {
    inner: NativeTransaction<'a>,
}

impl<'a> Txn<'a> for ReadonlyTransaction<'a> {
//     fn get_inner_txn<'b>(&'a self) -> &'a NativeTransaction<'a> {
//         &self.inner
//     }
    fn get_handle(&self) -> *mut ffi::MDB_txn {
        self.inner.handle
    }
    fn get_env(&self) -> &'a Environment {
        self.inner.env
    }
    fn get_state(&self) -> TransactionState {
        self.inner.state
    }

}

impl<'a> ReadonlyTransaction<'a> {
    pub fn new_with_native(txn: NativeTransaction<'a>) -> ReadonlyTransaction<'a> {
        ReadonlyTransaction {
            inner: txn,
        }
    }

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(ffi::MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))

    }

    /// Aborts transaction. But readonly transaction could be
    /// reused later by calling `renew`
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by call to `renew`
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Acquires a new reader lock after transaction
    /// `abort` or `reset`
    pub fn renew(&mut self) -> MdbResult<()> {
        self.inner.renew()
    }

    pub fn bind(&self, db_handle: DbHandle) -> Database {
        Database::new_with_handle(db_handle.handle)
    }
}