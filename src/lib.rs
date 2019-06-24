#![allow(trivial_casts)]
#![allow(trivial_numeric_casts)]

extern crate libc;

#[macro_use] extern crate bitflags;
#[macro_use] extern crate log;

extern crate liblmdb_sys as ffi;

pub use libc::c_int;
pub use ffi::{mdb_filehandle_t, MDB_stat, MDB_envinfo, MDB_val};
pub use environment::{EnvBuilder, Environment, EnvFlags, EnvCreateFlags};
pub use database::{Database, DbFlags, DbHandle};
pub use core::{MdbError, MdbValue};
pub use transaction::{Transaction, ReadonlyTransaction, Txn };
pub use cursor::{Cursor, CursorValue, CursorIter, CursorKeyRangeIter};
pub use traits::{FromMdbValue, ToMdbValue};

#[macro_use]
pub mod core;
pub mod environment;
pub mod transaction;
pub mod database;
pub mod cursor;
pub mod traits;
mod utils;

#[cfg(test)]
mod tests;
