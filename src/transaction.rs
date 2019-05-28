#![allow(non_upper_case_globals)]

use libc::{c_uint};
use std;
use std::ptr;

use ffi::{self};
use traits::{ToMdbValue, FromMdbValue};

use crate::core::{ MdbError, MdbResult, MdbValue, StateError };
use crate::cursor::{ Cursor };
use crate::database::{ Database, DbHandle};
use crate::environment::{ Environment };


#[derive(Copy, PartialEq, Debug, Eq, Clone)]
enum TransactionState {
    Normal,   // Normal, any operation possible
    Released, // Released (reset on readonly), has to be renewed
    Invalid,  // Invalid, no further operation possible
}

#[derive(Debug)]
pub struct NativeTransaction<'a> {
    pub handle: *mut ffi::MDB_txn,
    env: &'a Environment,
    flags: usize,
    state: TransactionState,
}

impl<'a> NativeTransaction<'a> {
    pub fn new_with_handle(h: *mut ffi::MDB_txn, flags: usize, env: &Environment) -> NativeTransaction {
        // debug!("new native txn");
        NativeTransaction {
            handle: h,
            flags: flags,
            state: TransactionState::Normal,
            env: env,
        }
    }

    fn is_readonly(&self) -> bool {
        (self.flags as u32 & ffi::MDB_RDONLY) == ffi::MDB_RDONLY
    }

    pub fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        debug!("commit txn");
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
            debug!("abort txn");
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
            debug!("silent abort");
            unsafe {ffi::mdb_txn_abort(self.handle);}
            self.state = TransactionState::Invalid;
        }
    }

    fn get_value<V: FromMdbValue + 'a>(&'a self, db: ffi::MDB_dbi, key: &ToMdbValue) -> MdbResult<V> {
        let mut key_val = key.to_mdb_value();
        unsafe {
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(ffi::mdb_get(self.handle, db, &mut key_val.value, &mut data_val.value));
            Ok(FromMdbValue::from_mdb_value(&data_val))
        }
    }

    pub fn get<V: FromMdbValue + 'a>(&'a self, db: ffi::MDB_dbi, key: &ToMdbValue) -> MdbResult<V> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.get_value(db, key)
    }

    fn set_value(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue, flags: c_uint) -> MdbResult<()> {
        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = value.to_mdb_value();

            lift_mdb!(ffi::mdb_put(self.handle, db, &mut key_val.value, &mut data_val.value, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    pub fn set(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.set_value(db, key, value)
    }

    pub fn append(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.set_value_with_flags(db, key, value, ffi::MDB_APPEND)
    }

    pub fn append_duplicate(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.set_value_with_flags(db, key, value, ffi::MDB_APPENDDUP)
    }

    /// Set the value for key only if the key does not exist in the database,
    /// even if the database supports duplicates.
    pub fn insert(&self, db: ffi::MDB_dbi, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.set_value_with_flags(db, key, value, ffi::MDB_NOOVERWRITE)
    }

    /// Deletes all values by key
    fn del_value(&self, db: ffi::MDB_dbi, key: &ToMdbValue) -> MdbResult<()> {
        unsafe {
            let mut key_val = key.to_mdb_value();
            lift_mdb!(ffi::mdb_del(self.handle, db, &mut key_val.value, ptr::null_mut()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_item(&self, db: ffi::MDB_dbi, key: &ToMdbValue, data: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = data.to_mdb_value();

            lift_mdb!(ffi::mdb_del(self.handle, db, &mut key_val.value, &mut data_val.value))
        }
    }

    /// Deletes all values for key
    pub fn del(&self, db: ffi::MDB_dbi, key: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.del_value(db, key)
    }

    /// Creates a new cursor in current transaction tied to db
    pub fn new_cursor(&'a self, db: ffi::MDB_dbi) -> MdbResult<Cursor<'a>> {
        Cursor::new(self, db)
    }

    /// Deletes provided database completely
    pub fn del_db(&self, db: Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            self.env.drop_db_from_cache(db.handle);
            lift_mdb!(ffi::mdb_drop(self.handle, db.handle, 1))
        }
    }

    /// Empties provided database
    pub fn clear_db(&self, db: ffi::MDB_dbi) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            lift_mdb!(ffi::mdb_drop(self.handle, db, 0))
        }
    }

    /// Retrieves provided database's statistics
    pub fn stat(&self, db: ffi::MDB_dbi) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_stat(self.handle, db, &mut tmp)}, tmp)
    }

    /*
    fn get_db(&self, name: &str, flags: DbFlags) -> MdbResult<Database> {
        self.env.get_db(name, flags)
            .and_then(|db| Ok(Database::new_with_handle(db.handle, self)))
    }
    */

    /*
    fn get_or_create_db(&self, name: &str, flags: DbFlags) -> MdbResult<Database> {
        self.get_db(name, flags | DbCreate)
    }
    */
}

impl<'a> Drop for NativeTransaction<'a> {
    fn drop(&mut self) {
        //debug!("Dropping native transaction!");
        self.silent_abort();
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    inner: NativeTransaction<'a>,
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

    pub fn bind(&self, db_handle: &DbHandle) -> Database {
        Database::new_with_handle(db_handle.handle, &self.inner)
    }
}


#[derive(Debug)]
pub struct ReadonlyTransaction<'a> {
    inner: NativeTransaction<'a>,
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

    pub fn bind(&self, db_handle: &DbHandle) -> Database {
        Database::new_with_handle(db_handle.handle, &self.inner)
    }
}