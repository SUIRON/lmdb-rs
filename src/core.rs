//! High level wrapper of LMDB APIs
//!
//! Requires knowledge of LMDB terminology
//!
//! # Environment
//!
//! Environment is actually the center point of LMDB, it's a container
//! of everything else. As some settings couldn't be adjusted after
//! opening, `Environment` is constructed using `EnvBuilder`, which
//! sets up maximum size, maximum count of named databases, maximum
//! readers which could be used from different threads without locking
//! and so on.
//!
//! # Database
//!
//! Actual key-value store. The most crucial aspect is whether a database
//! allows duplicates or not. It is specified on creation and couldn't be
//! changed later. Entries for the same key are called `items`.
//!
//! There are a couple of optmizations to use, like marking
//! keys or data as integer, allowing sorting using reverse key, marking
//! keys/data as fixed size.
//!
//! # Transaction
//!
//! Absolutely every db operation happens in a transaction. It could
//! be a read-only transaction (reader), which is lockless and therefore
//! cheap. Or it could be a read-write transaction, which is unique, i.e.
//! there could be only one writer at a time.
//!
//! While readers are cheap and lockless, they work better being short-lived
//! as in other case they may lock pages from being reused. Readers have
//! a special API for marking as finished and renewing.
//!
//! It is perfectly fine to create nested transactions.
//!
//!
//! # Example
//!

use libc::{c_int, size_t, c_void};
use std;
use std::error::Error;
use std::result::Result;
use std::mem;
use ffi::{self, MDB_val};
pub use MdbError::{NotFound, KeyExists, Other, StateError, Corrupted, Panic};
pub use MdbError::{InvalidPath, TxnFull, CursorFull, PageFull, CacheError};
use utils::{error_msg};

macro_rules! lift_mdb {
    ($e:expr) => (lift_mdb!($e, ()));
    ($e:expr, $r:expr) => (
        {
            let t = $e;
            match t {
                ffi::MDB_SUCCESS => Ok($r),
                _ => return Err(MdbError::new_with_code(t))
            }
        })
}

macro_rules! try_mdb {
        ($e:expr) => (
        {
            let t = $e;
            match t {
                ffi::MDB_SUCCESS => (),
                _ => return Err(MdbError::new_with_code(t))
            }
        })
}

macro_rules! assert_state_eq {
    ($log:ident, $cur:expr, $exp:expr) =>
        ({
            let c = $cur;
            let e = $exp;
            if c == e {
                ()
            } else {
                let msg = format!("{} requires {:?}, is in {:?}", stringify!($log), c, e);
                return Err(StateError(msg))
            }})
}

/// MdbError wraps information about LMDB error
#[derive(Debug)]
pub enum MdbError {
    NotFound,
    KeyExists,
    TxnFull,
    CursorFull,
    PageFull,
    Corrupted,
    Panic,
    InvalidPath,
    StateError(String),
    CacheError,
    Other(c_int, String)
}


impl MdbError {
    pub fn new_with_code(code: c_int) -> MdbError {
        match code {
            ffi::MDB_NOTFOUND    => NotFound,
            ffi::MDB_KEYEXIST    => KeyExists,
            ffi::MDB_TXN_FULL    => TxnFull,
            ffi::MDB_CURSOR_FULL => CursorFull,
            ffi::MDB_PAGE_FULL   => PageFull,
            ffi::MDB_CORRUPTED   => Corrupted,
            ffi::MDB_PANIC       => Panic,
            _                    => Other(code, error_msg(code))
        }
    }
}


impl std::fmt::Display for MdbError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &NotFound | &KeyExists | &TxnFull |
            &CursorFull | &PageFull | &Corrupted |
            &Panic | &InvalidPath | &CacheError => write!(fmt, "{}", self.description()),
            &StateError(ref msg) => write!(fmt, "{}", msg),
            &Other(code, ref msg) => write!(fmt, "{}: {}", code, msg)
        }
    }
}

impl Error for MdbError {
    fn description(&self) -> &'static str {
        match self {
            &NotFound => "not found",
            &KeyExists => "key exists",
            &TxnFull => "txn full",
            &CursorFull => "cursor full",
            &PageFull => "page full",
            &Corrupted => "corrupted",
            &Panic => "panic",
            &InvalidPath => "invalid path for database",
            &StateError(_) => "state error",
            &CacheError => "db cache error",
            &Other(_, _) => "other error",
        }
    }
}


pub type MdbResult<T> = Result<T, MdbError>;

#[derive(Copy, Clone, Debug)]
pub struct MdbValue<'a> {
    pub value: MDB_val,
    pub marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> MdbValue<'a> {
    #[inline]
    pub unsafe fn new(data: *const c_void, len: usize) -> MdbValue<'a> {
        MdbValue {
            value: MDB_val {
                mv_data: data,
                mv_size: len as size_t
            },
            marker: ::std::marker::PhantomData
        }
    }

    #[inline]
    pub unsafe fn from_raw(mdb_val: *const ffi::MDB_val) -> MdbValue<'a> {
        MdbValue::new((*mdb_val).mv_data, (*mdb_val).mv_size as usize)
    }

    #[inline]
    pub fn new_from_sized<T>(data: &'a T) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(mem::transmute(data), mem::size_of::<T>())
        }
    }

    #[inline]
    pub unsafe fn get_ref(&'a self) -> *const c_void {
        self.value.mv_data
    }

    #[inline]
    pub fn get_size(&self) -> usize {
        self.value.mv_size as usize
    }
}
