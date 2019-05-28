use libc::{c_int, c_uint, };
use ffi::{self, MDB_val};
use traits::{ToMdbValue, FromMdbValue};

use crate::core::{ MdbError, MdbResult };
use crate::transaction::{ NativeTransaction };
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
pub struct Database<'a> {
    pub handle: ffi::MDB_dbi,
    txn: &'a NativeTransaction<'a>,
}

// FIXME: provide different interfaces for read-only/read-write databases
// FIXME: provide different interfaces for simple KV and storage with duplicates

impl<'a> Database<'a> {
    pub fn new_with_handle(handle: ffi::MDB_dbi, txn: &'a NativeTransaction<'a>) -> Database<'a> {
        Database { handle: handle, txn: txn }
    }

    /// Retrieves current db's statistics.
    pub fn stat(&'a self) -> MdbResult<ffi::MDB_stat> {
        self.txn.stat(self.handle)
    }

    /// Retrieves a value by key. In case of DbAllowDups it will be the first value
    pub fn get<V: FromMdbValue + 'a>(&'a self, key: &ToMdbValue) -> MdbResult<V> {
        self.txn.get(self.handle, key)
    }

    /// Sets value for key. In case of DbAllowDups it will add a new item
    pub fn set(&self, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.txn.set(self.handle, key, value)
    }

    /// Appends new key-value pair to database, starting a new page instead of splitting an
    /// existing one if necessary. Requires that key be >= all existing keys in the database
    /// (or will return KeyExists error).
    pub fn append<K: ToMdbValue, V: ToMdbValue>(&self, key: &K, value: &V) -> MdbResult<()> {
        self.txn.append(self.handle, key, value)
    }

    /// Appends new value for the given key (requires DbAllowDups), starting a new page instead
    /// of splitting an existing one if necessary. Requires that value be >= all existing values
    /// for the given key (or will return KeyExists error).
    pub fn append_duplicate<K: ToMdbValue, V: ToMdbValue>(&self, key: &K, value: &V) -> MdbResult<()> {
        self.txn.append_duplicate(self.handle, key, value)
    }

    /// Set value for key. Fails if key already exists, even when duplicates are allowed.
    pub fn insert(&self, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.txn.insert(self.handle, key, value)
    }

    /// Deletes value for key.
    pub fn del(&self, key: &ToMdbValue) -> MdbResult<()> {
        self.txn.del(self.handle, key)
    }

    /// Should be used only with DbAllowDups. Deletes corresponding (key, value)
    pub fn del_item(&self, key: &ToMdbValue, data: &ToMdbValue) -> MdbResult<()> {
        self.txn.del_item(self.handle, key, data)
    }

    /// Returns a new cursor
    pub fn new_cursor(&'a self) -> MdbResult<Cursor<'a>> {
        self.txn.new_cursor(self.handle)
    }

    /// Deletes current db, also moves it out
    pub fn del_db(self) -> MdbResult<()> {
        self.txn.del_db(self)
    }

    /// Removes all key/values from db
    pub fn clear(&self) -> MdbResult<()> {
        self.txn.clear_db(self.handle)
    }

    /// Returns an iterator for all values in database
    pub fn iter(&'a self) -> MdbResult<CursorIterator<'a, CursorIter>> {
        self.txn.new_cursor(self.handle)
            .and_then(|c| Ok(CursorIterator::wrap(c, CursorIter)))
    }

    /// Returns an iterator through keys starting with start_key (>=), start_key is included
    pub fn keyrange_from<'c, K: ToMdbValue + 'c>(&'c self, start_key: &'c K) -> MdbResult<CursorIterator<'c, CursorFromKeyIter>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorFromKeyIter::new(start_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys less than end_key, end_key is not included
    pub fn keyrange_to<'c, K: ToMdbValue + 'c>(&'c self, end_key: &'c K) -> MdbResult<CursorIterator<'c, CursorToKeyIter>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorToKeyIter::new(end_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys `start_key <= x < end_key`. This is, start_key is
    /// included in the iteration, while end_key is kept excluded.
    pub fn keyrange_from_to<'c, K: ToMdbValue + 'c>(&'c self, start_key: &'c K, end_key: &'c K)
                               -> MdbResult<CursorIterator<'c, CursorKeyRangeIter>>
    {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, false);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for values between start_key and end_key (included).
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with ffi::MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'c, K: ToMdbValue + 'c>(&'c self, start_key: &'c K, end_key: &'c K)
                               -> MdbResult<CursorIterator<'c, CursorKeyRangeIter>>
    {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, true);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for all items (i.e. values with same key)
    pub fn item_iter<'c, 'db: 'c, K: ToMdbValue>(&'db self, key: &'c K) -> MdbResult<CursorIterator<'c, CursorItemIter<'c>>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let inner_iter = CursorItemIter::<'c>::new(key);
        Ok(CursorIterator::<'c>::wrap(cursor, inner_iter))
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
    pub fn set_compare(&self, cmp_fn: extern "C" fn(*const MDB_val, *const MDB_val) -> c_int) -> MdbResult<()> {
        lift_mdb!(unsafe {
            ffi::mdb_set_compare(self.txn.handle, self.handle, cmp_fn)
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
    pub fn set_dupsort(&self, cmp_fn: extern "C" fn(*const MDB_val, *const MDB_val) -> c_int) -> MdbResult<()> {
        lift_mdb!(unsafe {
            ffi::mdb_set_dupsort(self.txn.handle, self.handle, cmp_fn)
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
