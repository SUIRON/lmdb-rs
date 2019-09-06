use libc::{c_int, c_uint, };
use ffi::{self, MDB_val};
use traits::{ToMdbValue, FromMdbValue};
use std::ptr;

use crate::core::{ MdbError, MdbResult, MdbValue, StateError };
use crate::transaction::{ TransactionState, Txn };
use crate::cursor::{ Cursor, CursorFromKeyIter, CursorItemIter, CursorIter, CursorIterator, CursorKeyRangeIter, CursorToKeyIter };

bitflags! {
    #[doc = "A set of database flags"]

    pub flags DbFlags: c_uint {
        #[doc="Keys are strings to be compared in reverse order, from the"]
        #[doc=" end of the strings to the beginning. By default, Keys are"]
        #[doc=" treated as strings and compared from beginning to end."]
        const DB_REVERSE_KEY   = ffi::MDB_REVERSEKEY,
        #[doc="Duplicate keys may be used in the database. (Or, from another"]
        #[doc="perspective, keys may have multiple data items, stored in sorted"]
        #[doc="order.) By default keys must be unique and may have only a"]
        #[doc="single data item."]
        const DB_ALLOW_DUPS    = ffi::MDB_DUPSORT,
        #[doc="Keys are binary integers in native byte order. Setting this"]
        #[doc="option requires all keys to be the same size, typically"]
        #[doc="sizeof(int) or sizeof(size_t)."]
        const DB_INT_KEY       = ffi::MDB_INTEGERKEY,
        #[doc="This flag may only be used in combination with"]
        #[doc="ffi::MDB_DUPSORT. This option tells the library that the data"]
        #[doc="items for this database are all the same size, which allows"]
        #[doc="further optimizations in storage and retrieval. When all data"]
        #[doc="items are the same size, the ffi::MDB_GET_MULTIPLE and"]
        #[doc="ffi::MDB_NEXT_MULTIPLE cursor operations may be used to retrieve"]
        #[doc="multiple items at once."]
        const DB_DUP_FIXED     = ffi::MDB_DUPFIXED,
        #[doc="This option specifies that duplicate data items are also"]
        #[doc="integers, and should be sorted as such."]
        const DB_ALLOW_INT_DUPS = ffi::MDB_INTEGERDUP,
        #[doc="This option specifies that duplicate data items should be"]
        #[doc=" compared as strings in reverse order."]
        const DB_REVERSE_DUPS = ffi::MDB_REVERSEDUP,
        #[doc="Create the named database if it doesn't exist. This option"]
        #[doc=" is not allowed in a read-only transaction or a read-only"]
        #[doc=" environment."]
        const DB_CREATE       = ffi::MDB_CREATE,
    }
}

/// Database
#[derive(Debug)]
pub struct Database {
    pub handle: ffi::MDB_dbi,
}

// FIXME: provide different interfaces for read-only/read-write databases
// FIXME: provide different interfaces for simple KV and storage with duplicates

impl Database {
    pub fn new_with_handle(handle: ffi::MDB_dbi) -> Database {
        Database { handle }
    }

    /// Retrieves current db's statistics.
    pub fn stat<'txn>(&self, txn: &'_ dyn Txn<'txn>) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_stat(txn.get_handle(), self.handle, &mut tmp)}, tmp)
    }

    fn get_value<'txn, V: FromMdbValue + 'txn>(&self, key: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<V> {
        let mut key_val = key.to_mdb_value();
        unsafe {
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(ffi::mdb_get(txn.get_handle(), self.handle, &mut key_val.value, &mut data_val.value));
            Ok(FromMdbValue::from_mdb_value(&data_val))
        }
    }

    /// Retrieves a value by key. In case of DbAllowDups it will be the first value
    pub fn get<'txn, V: FromMdbValue + 'txn>(&self, key: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<V> {


        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.get_value(key, txn)
    }

    fn set_value<'txn>(&self, key: &dyn ToMdbValue, value: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {
        self.set_value_with_flags(key, value, 0, txn)
    }

    fn set_value_with_flags<'txn>(&self, key: &dyn ToMdbValue, value: &dyn ToMdbValue, flags: c_uint, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = value.to_mdb_value();

            lift_mdb!(ffi::mdb_put(txn.get_handle(), self.handle, &mut key_val.value, &mut data_val.value, flags))
        }
    }

    /// Sets value for key. In case of DbAllowDups it will add a new item
    pub fn set<'txn>(&self, key: &dyn ToMdbValue, value: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {


        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.set_value(key, value, txn)
    }

    /// Appends new key-value pair to database, starting a new page instead of splitting an
    /// existing one if necessary. Requires that key be >= all existing keys in the database
    /// (or will return KeyExists error).
    pub fn append<'txn, K: ToMdbValue, V: ToMdbValue>(&self, key: &K, value: &V, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.set_value_with_flags(key, value, ffi::MDB_APPEND, txn)
    }

    /// Appends new value for the given key (requires DbAllowDups), starting a new page instead
    /// of splitting an existing one if necessary. Requires that value be >= all existing values
    /// for the given key (or will return KeyExists error).
    pub fn append_duplicate<'txn, K: ToMdbValue, V: ToMdbValue>(&self, key: &K, value: &V, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.set_value_with_flags(key, value, ffi::MDB_APPENDDUP, txn)
    }

    /// Set value for key. Fails if key already exists, even when duplicates are allowed.
    pub fn insert<'txn>(&self, key: &dyn ToMdbValue, value: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.set_value_with_flags(key, value, ffi::MDB_NOOVERWRITE, txn)
    }

    fn del_value<'txn>(&self, key: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        unsafe {
            let mut key_val = key.to_mdb_value();
            lift_mdb!(ffi::mdb_del(txn.get_handle(), self.handle, &mut key_val.value, ptr::null_mut()))
        }
    }

    /// Deletes value for key.
    pub fn del<'txn>(&self, key: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        self.del_value(key, txn)
    }

    /// Should be used only with DbAllowDups. Deletes corresponding (key, value)
    pub fn del_item<'txn>(&self, key: &dyn ToMdbValue, data: &dyn ToMdbValue, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = data.to_mdb_value();

            lift_mdb!(ffi::mdb_del(txn.get_handle(), self.handle, &mut key_val.value, &mut data_val.value))
        }
    }

    /// Returns a new cursor
    pub fn new_cursor<'c, 'txn>(&self, txn: &'c dyn Txn<'txn>) -> MdbResult<Cursor<'c, 'txn>> {

        Cursor::new(txn, self.handle)
    }

    /// Deletes current db, also moves it out
    pub fn del_db<'txn>(self, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        unsafe {
            txn.get_env().drop_db_from_cache(self.handle);
            lift_mdb!(ffi::mdb_drop(txn.get_handle(), self.handle, 1))
        }
    }

    /// Removes all key/values from db
    pub fn clear<'txn>(&self, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        assert_state_eq!(txn, txn.get_state(), TransactionState::Normal);
        unsafe {
            lift_mdb!(ffi::mdb_drop(txn.get_handle(), self.handle, 0))
        }
    }

    /// Returns an iterator for all values in database
    pub fn iter<'c, 'txn>(&self, txn: &'c dyn Txn<'txn>) -> MdbResult<CursorIterator<'c, 'txn, CursorIter>> {
        self.new_cursor(txn)
            .and_then(|c| Ok(CursorIterator::wrap(c, CursorIter)))
    }

    /// Returns an iterator through keys starting with start_key (>=), start_key is included
    pub fn keyrange_from<'c, 'txn, K: ToMdbValue + 'c>(&'c self, start_key: &'c K, txn: &'c dyn Txn<'txn>) -> MdbResult<CursorIterator<'c, 'txn, CursorFromKeyIter>> {
        let cursor = try!(self.new_cursor(txn));
        let key_range = CursorFromKeyIter::new(start_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys less than end_key, end_key is not included
    pub fn keyrange_to<'c, 'txn, K: ToMdbValue + 'c>(&'c self, end_key: &'c K, txn: &'c dyn Txn<'txn>) -> MdbResult<CursorIterator<'c, 'txn, CursorToKeyIter>> {
        let cursor = try!(self.new_cursor(txn));
        let key_range = CursorToKeyIter::new(end_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys `start_key <= x < end_key`. This is, start_key is
    /// included in the iteration, while end_key is kept excluded.
    pub fn keyrange_from_to<'c, 'txn, K: ToMdbValue + 'c>(&'c self, start_key: &'c K, end_key: &'c K, txn: &'c dyn Txn<'txn>)
                               -> MdbResult<CursorIterator<'c, 'txn, CursorKeyRangeIter>>
    {
        let cursor = try!(self.new_cursor(txn));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, false);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for values between start_key and end_key (included).
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with ffi::MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'c, 'txn, K: ToMdbValue + 'c>(&'c self, start_key: &'c K, end_key: &'c K, txn: &'c dyn Txn<'txn>)
                               -> MdbResult<CursorIterator<'c, 'txn, CursorKeyRangeIter>>
    {
        let cursor = try!(self.new_cursor(txn));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, true);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for all items (i.e. values with same key)
    pub fn item_iter<'c, 'txn, 'db: 'c, K: ToMdbValue>(&'db self, key: &'c K, txn: &'c dyn Txn<'txn>) -> MdbResult<CursorIterator<'c, 'txn, CursorItemIter<'c>>> {
        let cursor = try!(self.new_cursor(txn));
        let inner_iter = CursorItemIter::<'c>::new(key);
        Ok(CursorIterator::<'c, 'txn>::wrap(cursor, inner_iter))
    }

    /// Sets the key compare function for this database.
    ///
    /// Warning: This function must be called before any data access functions
    /// are used, otherwise data corruption may occur. The same comparison
    /// function must be used by every program accessing the database, every
    /// time the database is used.
    ///
    /// If not called, keys are compared lexically, with shorter keys collating
    /// before longer keys.
    ///
    /// Setting lasts for the lifetime of the underlying db handle.
    pub fn set_compare<'txn>(&self, cmp_fn: extern "C" fn(*const MDB_val, *const MDB_val) -> c_int, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {

        lift_mdb!(unsafe {
            ffi::mdb_set_compare(txn.get_handle(), self.handle, cmp_fn)
        })
    }

    /// Sets the value comparison function for values of the same key in this database.
    ///
    /// Warning: This function must be called before any data access functions
    /// are used, otherwise data corruption may occur. The same dupsort
    /// function must be used by every program accessing the database, every
    /// time the database is used.
    ///
    /// If not called, values are compared lexically, with shorter values collating
    /// before longer values.
    ///
    /// Only used when DbAllowDups is true.
    /// Setting lasts for the lifetime of the underlying db handle.
    pub fn set_dupsort<'txn>(&self, cmp_fn: extern "C" fn(*const MDB_val, *const MDB_val) -> c_int, txn: &'_ dyn Txn<'txn>) -> MdbResult<()> {


        lift_mdb!(unsafe {
            ffi::mdb_set_dupsort(txn.get_handle(), self.handle, cmp_fn)
        })
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
/// A handle to a database
///
/// It can be cached to avoid opening db on every access
/// In the current state it is unsafe as other thread
/// can ask to drop it.
pub struct DbHandle {
    pub handle: ffi::MDB_dbi,
    pub flags: DbFlags
}

unsafe impl Sync for DbHandle {}
unsafe impl Send for DbHandle {}
