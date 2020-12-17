use libc::{c_uint, size_t};
use std;
use std::cmp::{Ordering};
use std::ptr;
use std::mem;
use ffi::{self};
use crate::traits::{ToMdbValue, FromMdbValue};

use crate::transaction::{ Txn };
use crate::core::{ MdbError, MdbResult, MdbValue };

/// Helper to determine the property of "less than or equal to" where
/// the "equal to" part is to be specified at runtime.
trait IsLess {
    fn is_less(&self, or_equal: bool) -> bool;
}

impl IsLess for Ordering {
    fn is_less(&self, or_equal: bool) -> bool {
        match (*self, or_equal) {
            (Ordering::Less, _) => true,
            (Ordering::Equal, true) => true,
            _ => false,
        }
    }
}

impl IsLess for MdbResult<Ordering> {
    fn is_less(&self, or_equal: bool) -> bool {
        match *self {
            Ok(ord) => ord.is_less(or_equal),
            Err(_) => false,
        }
    }
}

#[derive(Debug)]
pub struct Cursor<'c, 'txn> {
    handle: *mut ffi::MDB_cursor,
    data_val: ffi::MDB_val,
    key_val: ffi::MDB_val,
    txn: &'c dyn Txn<'txn>,
    db: ffi::MDB_dbi,
    valid_key: bool,
    valid_value: bool,
}

impl<'c, 'txn> Cursor<'c, 'txn> {
    pub fn new(txn: &'c dyn Txn<'txn>, db: ffi::MDB_dbi) -> MdbResult<Cursor<'c, 'txn>> {
        //debug!("Opening cursor in {}", db);
        let mut tmp: *mut ffi::MDB_cursor = std::ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_cursor_open(txn.get_handle(), db, &mut tmp) });
        Ok(Cursor {
            handle: tmp,
            data_val: unsafe { std::mem::zeroed() },
            key_val: unsafe { std::mem::zeroed() },
            txn,
            db,
            valid_key: false,
            valid_value: false,
        })
    }

    fn navigate(&mut self, op: ffi::MDB_cursor_op) -> MdbResult<()> {
        self.valid_key = false;
        self.valid_value = false;

        let res = unsafe {
            ffi::mdb_cursor_get(self.handle, &mut self.key_val, &mut self.data_val, op)
        };
        match res {
            ffi::MDB_SUCCESS => {
                // MDB_SET is the only cursor operation which doesn't
                // write back a new value. In this case any access to
                // cursor key value should cause a cursor retrieval
                // to get back pointer to database owned memory instead
                // of value used to set the cursor as it might be
                // already destroyed and there is no need to borrow it
                self.valid_key = op != ffi::MDB_cursor_op::MDB_SET;
                self.valid_value = op != ffi::MDB_cursor_op::MDB_GET_BOTH_RANGE;
                Ok(())
            },
            e => Err(MdbError::new_with_code(e))
        }
    }

    fn move_to<K, V>(&mut self, key: &K, value: Option<&V>, op: ffi::MDB_cursor_op) -> MdbResult<()>
        where K: ToMdbValue, V: ToMdbValue {
        self.key_val = key.to_mdb_value().value;
        self.data_val = match value {
            Some(v) => v.to_mdb_value().value,
            _ => unsafe {std::mem::zeroed() }
        };

        self.navigate(op)
    }

    fn _move_to_prev<K>(&mut self, key: &K) -> MdbResult<()>
        where K: ToMdbValue {
        self.key_val = key.to_mdb_value().value;
        self.data_val = unsafe {std::mem::zeroed()};
        let mut original_key = key.to_mdb_value().value;

        self.valid_key = false;
        self.valid_value = false;

        let res = unsafe {
            ffi::mdb_cursor_get(self.handle, &mut self.key_val, &mut self.data_val, ffi::MDB_cursor_op::MDB_SET_RANGE)
        };
        if res == ffi::MDB_NOTFOUND || res == ffi::MDB_SUCCESS {
            if unsafe {ffi::mdb_cmp(self.txn.get_handle(), self.db, &mut original_key, &mut self.key_val) < 0 || res == ffi::MDB_NOTFOUND } {
                let res = unsafe {
                    ffi::mdb_cursor_get(self.handle, &mut self.key_val, &mut self.data_val, ffi::MDB_cursor_op::MDB_PREV_NODUP)
                };
                match res {
                    ffi::MDB_SUCCESS => {
                        self.valid_key = true;
                        self.valid_value = true;
                        return Ok(())
                    },
                    _ => return Err(MdbError::new_with_code(res))
                }
            }
            if res == ffi::MDB_SUCCESS {
                self.valid_key = true;
                self.valid_value = true;
                return Ok(())
            }
        }
        Err(MdbError::new_with_code(res))
    }

    /// Moves cursor to first entry
    pub fn move_to_first(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn move_to_last(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn move_to_key<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&MdbValue<'k>>, ffi::MDB_cursor_op::MDB_SET_KEY)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to key
    pub fn move_to_gte_key<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&MdbValue<'k>>, ffi::MDB_cursor_op::MDB_SET_RANGE)
    }

    /// Moves cursor to first entry for key less than
    /// or equal to key
    /// when the database supports dup-keys this will point the cursor to the last item of
    /// the previous key
    pub fn move_to_lte_key<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        self._move_to_prev(key)
    }

    /// Moves cursor to first entry for key less than
    /// or equal to ke
    /// when the database supports dup-keys this will point the cursor to the first item of
    /// the previous key
    pub fn move_to_lte_key_first_item<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        match self._move_to_prev(key) {
            Ok(_) => self.move_to_first_item(),
            Err(e) => Err(e)
        }
    }

    /// Moves cursor to first entry for key less than
    /// or equal to ke
    /// when the database supports dup-keys this will point the cursor to the first item of
    /// the previous key
    pub fn move_to_lte_key_and_item<'a, K, V>(&'a mut self, key: &K, value: &V) -> MdbResult<()> where K: ToMdbValue + FromMdbValue + 'a, V: ToMdbValue + FromMdbValue + 'a {
        match self.move_to_lte_key_first_item(key) {
            Ok(_) => {
                let key = self.get_key::<K>()?;
                self.move_to_lte_item(&key, value)?;
                self.valid_key = false;
                self.valid_value = false;
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    pub fn move_to_gte_key_and_item<'a, K, V>(&'a mut self, key: &K, value: &V) -> MdbResult<()> where K: ToMdbValue + FromMdbValue + 'a, V: ToMdbValue + FromMdbValue + 'a {
        match self.move_to_gte_key(key) {
            Ok(_) => {
                let key = self.get_key::<K>()?;
                self.move_to_gte_item(&key, value)?;
                self.valid_key = false;
                self.valid_value = false;
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    /// Moves cursor to specific item (for example, if cursor
    /// already points to a correct key and you need to delete
    /// a specific item through cursor)
    pub fn move_to_item<K, V>(&mut self, key: &K, value: & V) -> MdbResult<()> where K: ToMdbValue, V: ToMdbValue {
        self.move_to(key, Some(value), ffi::MDB_cursor_op::MDB_GET_BOTH)
    }

    /// Moves cursor (for the matching key) to nearest item, greater than or equal to the dup_key.
    pub fn move_to_gte_item<K, V>(&mut self, key: &K, value: & V) -> MdbResult<()> where K: ToMdbValue, V: ToMdbValue {
        self.move_to(key, Some(value), ffi::MDB_cursor_op::MDB_GET_BOTH_RANGE)?;
        self.valid_key = false;
        Ok(())
    }

    /// Moves cursor (for the matching key) to nearest item, less than or equal to the dup_key.
    pub fn move_to_lte_item<'a, K, V>(&'a mut self, key: &K, value: &V) -> MdbResult<()> where K: ToMdbValue, V: ToMdbValue + FromMdbValue+'a {
        match self.move_to_gte_item(key, value) {
            Ok(_) | Err(MdbError::NotFound) => {
                let mut old_value = value.to_mdb_value().value;
                match self.get_value::<V>() {
                    Ok(val) => if unsafe { ffi::mdb_dcmp(self.txn.get_handle(), self.db, &mut old_value, &mut val.to_mdb_value().value) < 0 } {
                        return self.move_to_prev_item();
                    },
                    Err(MdbError::NotFound) => return self.move_to_prev_item(),
                    Err(e) => return Err(e)
                }
                Ok(())
            },
            Err(e) => Err(e)
        }
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn move_to_next_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn move_to_next_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn move_to_prev_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item
    pub fn move_to_prev(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_PREV)
    }

    /// Moves cursor to next item
    pub fn move_to_next(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_NEXT)
    }

    pub fn move_to_prev_key_dup(&mut self) -> MdbResult<()> {
        match self.navigate(ffi::MDB_cursor_op::MDB_PREV_NODUP) {
            Ok(_) => self.move_to_first_item(),
            Err(e) => Err(e)
        }
    }

    /// Moves cursor to prev item with the same key as current
    pub fn move_to_prev_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn move_to_first_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn move_to_last_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_LAST_DUP)
    }

    /// Retrieves current key/value as tuple
    pub fn get<'a, T: FromMdbValue + 'a, U: FromMdbValue + 'a>(&'a mut self) -> MdbResult<(T, U)> {
        let (k, v) = self.get_plain()?;

        unsafe {
            Ok((FromMdbValue::from_mdb_value(mem::transmute(&k)),
                FromMdbValue::from_mdb_value(mem::transmute(&v))))
        }
    }

    /// Retrieves current value
    pub fn get_value<'a, V: FromMdbValue + 'a>(&'a mut self) -> MdbResult<V> {
        let (_, v) = self.get_plain()?;

        unsafe {
            Ok(FromMdbValue::from_mdb_value(mem::transmute(&v)))
        }
    }

    /// Retrieves current key
    pub fn get_key<'a, K: FromMdbValue + 'a>(&'a mut self) -> MdbResult<K> {
        let (k, _) = self.get_plain()?;

        unsafe {
            Ok(FromMdbValue::from_mdb_value(mem::transmute(&k)))
        }
    }

    /// Compares the cursor's current key with the specified other one.
    #[inline]
    fn cmp_key(&mut self, other: &MdbValue) -> MdbResult<Ordering> {
        let (k, _) = self.get_plain()?;
        let mut kval = k.value;
        let cmp = unsafe {
            ffi::mdb_cmp(self.txn.get_handle(), self.db, &mut kval, other as *const MdbValue<'_> as *mut ffi::MDB_val)
        };
        Ok(match cmp {
            n if n < 0 => Ordering::Less,
            n if n > 0 => Ordering::Greater,
            _          => Ordering::Equal,
        })
    }

    #[inline]
    fn ensure_key_valid(&mut self) -> MdbResult<()> {
        // If key might be invalid simply perform cursor get to be sure
        // it points to database memory instead of user one
        if !self.valid_key {
            unsafe {
                try_mdb!(ffi::mdb_cursor_get(self.handle, &mut self.key_val,
                                             ptr::null_mut(),
                                             ffi::MDB_cursor_op::MDB_GET_CURRENT));
            }
            self.valid_key = true;
        }
        Ok(())
    }

    #[inline]
    fn get_plain(&mut self) -> MdbResult<(MdbValue<'c>, MdbValue<'c>)> {
        self.ensure_key_valid()?;
        if !self.valid_value && self.valid_key {
            unsafe {
                try_mdb!(ffi::mdb_cursor_get(self.handle, ptr::null_mut(),
                                                &mut self.data_val,
                                                ffi::MDB_cursor_op::MDB_GET_CURRENT));
            }
            self.valid_value = true;
        }
        let k = MdbValue {value: self.key_val, marker: ::std::marker::PhantomData};
        let v = MdbValue {value: self.data_val, marker: ::std::marker::PhantomData};

        Ok((k, v))
    }

    #[allow(dead_code)]
    // This one is used for debugging, so it's to OK to leave it for a while
    fn dump_value(&self, prefix: &str) {
        if self.valid_key {
            println!("{}: key {:?}, data {:?}", prefix,
                     self.key_val,
                     self.data_val);
        }
    }

    fn set_value<V: ToMdbValue>(&mut self, value: &V, flags: c_uint) -> MdbResult<()> {
        self.ensure_key_valid()?;
        self.data_val = value.to_mdb_value().value;
        lift_mdb!(unsafe {ffi::mdb_cursor_put(self.handle, &mut self.key_val, &mut self.data_val, flags)})
    }

    pub fn set<K: ToMdbValue, V: ToMdbValue>(&mut self, key: &K, value: &V, flags: c_uint) -> MdbResult<()> {
        self.key_val = key.to_mdb_value().value;
        self.valid_key = true;
        let res = self.set_value(value, flags);
        self.valid_key = false;
        res
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn replace<V: ToMdbValue>(&mut self, value: &V) -> MdbResult<()> {
        let res = self.set_value(value, ffi::MDB_CURRENT);
        self.valid_key = false;
        res
    }

    /// Adds a new item when created with allowed duplicates
    pub fn add_item<V: ToMdbValue>(&mut self, value: &V) -> MdbResult<()> {
        let res = self.set_value(value, 0);
        self.valid_key = false;
        res
    }

    fn del_value(&mut self, flags: c_uint) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_cursor_del(self.handle, flags) })
    }

    /// Deletes current key
    pub fn del(&mut self) -> MdbResult<()> {
        self.del_all()
    }

    /// Deletes only current item
    ///
    /// Note that it doesn't check anything so it is caller responsibility
    /// to make sure that correct item is deleted if, for example, caller
    /// wants to delete only items of current key
    pub fn del_item(&mut self) -> MdbResult<()> {
        let res = self.del_value(0);
        self.valid_key = false;
        res
    }

    /// Deletes all items with same key as current
    pub fn del_all(&mut self) -> MdbResult<()> {
        self.del_value(ffi::MDB_NODUPDATA)
    }

    /// Returns count of items with the same key as current
    pub fn item_count(&self) -> MdbResult<size_t> {
        let mut tmp: size_t = 0;
        lift_mdb!(unsafe {ffi::mdb_cursor_count(self.handle, &mut tmp)}, tmp)
    }

    pub fn get_item<'k, K: ToMdbValue>(self, k: &'k K) -> CursorItemAccessor<'c, 'k, 'txn, K> {
        CursorItemAccessor {
            cursor: self,
            key: k
        }
    }
}

impl<'c, 'txn> Drop for Cursor<'c, 'txn> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.handle) };
    }
}

#[derive(Debug)]
pub struct CursorItemAccessor<'c, 'k, 'txn, K: 'k> {
    cursor: Cursor<'c, 'txn>,
    key: &'k K,
}

impl<'k, 'c: 'k, 'txn, K: ToMdbValue> CursorItemAccessor<'c, 'k, 'txn, K> {
    pub fn get<'a, V: FromMdbValue + 'a>(&'a mut self) -> MdbResult<V> {
        self.cursor.move_to_key(self.key)?;
        self.cursor.get_value()
    }

    pub fn add<V: ToMdbValue>(&mut self, v: &V) -> MdbResult<()> {
        self.cursor.set(self.key, v, 0)
    }

    pub fn del<V: ToMdbValue>(&mut self, v: &V) -> MdbResult<()> {
        self.cursor.move_to_item(self.key, v)?;
        self.cursor.del_item()
    }

    pub fn del_all(&mut self) -> MdbResult<()> {
        self.cursor.move_to_key(self.key)?;
        self.cursor.del_all()
    }

    pub fn into_inner(self) -> Cursor<'c, 'txn> {
        let tmp = self;
        tmp.cursor
    }
}

#[derive(Debug)]
pub struct CursorValue<'cursor> {
    key: MdbValue<'cursor>,
    value: MdbValue<'cursor>,
    marker: ::std::marker::PhantomData<&'cursor ()>,
}

/// CursorValue performs lazy data extraction from iterator
/// avoiding any data conversions and memory copy. Lifetime
/// is limited to iterator lifetime
impl<'cursor> CursorValue<'cursor> {
    pub fn get_key<T: FromMdbValue + 'cursor>(&'cursor self) -> T {
        FromMdbValue::from_mdb_value(&self.key)
    }

    pub fn get_value<T: FromMdbValue + 'cursor>(&'cursor self) -> T {
        FromMdbValue::from_mdb_value(&self.value)
    }

    pub fn get<T: FromMdbValue + 'cursor, U: FromMdbValue + 'cursor>(&'cursor self) -> (T, U) {
        (FromMdbValue::from_mdb_value(&self.key),
         FromMdbValue::from_mdb_value(&self.value))
    }
}

/// Allows the cration of custom cursor iteration behaviours.
pub trait IterateCursor {
    /// Returns true if initialization successful, for example that
    /// the key exists.
    fn init_cursor<'a, 'txn, 'b: 'a>(&'a self, cursor: &mut Cursor<'b, 'txn>) -> bool;

    /// Returns true if there is still data and iterator is in correct range
    fn move_to_next<'iter, 'cursor: 'iter, 'txn>(&'iter self, cursor: &'cursor mut Cursor<'cursor, 'txn>) -> bool;

    /// Returns size hint considering current state of cursor
    fn get_size_hint(&self, _cursor: &Cursor) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[derive(Debug)]
pub struct CursorIterator<'c, 'txn, I> {
    inner: I,
    has_data: bool,
    cursor: Cursor<'c, 'txn>,
    marker: ::std::marker::PhantomData<&'c ()>,
}

impl<'c, 'txn, I: IterateCursor + 'c> CursorIterator<'c, 'txn, I> {
    pub fn wrap(cursor: Cursor<'c, 'txn>, inner: I) -> CursorIterator<'c, 'txn, I> {
        let mut cursor = cursor;
        let has_data = inner.init_cursor(&mut cursor);
        CursorIterator {
            inner,
            has_data,
            cursor,
            marker: ::std::marker::PhantomData,
        }
    }

    #[allow(dead_code)]
    fn unwrap(self) -> Cursor<'c, 'txn> {
        self.cursor
    }
}

impl<'c, 'txn, I: IterateCursor + 'c> Iterator for CursorIterator<'c, 'txn, I> {
    type Item = CursorValue<'c>;

    fn next(&mut self) -> Option<CursorValue<'c>> {
        if !self.has_data {
            None
        } else {
            match self.cursor.get_plain() {
                Err(_) => None,
                Ok((k, v)) => {
                    self.has_data = unsafe { self.inner.move_to_next(mem::transmute(&mut self.cursor)) };
                    Some(CursorValue {
                        key: k,
                        value: v,
                        marker: ::std::marker::PhantomData
                    })
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.get_size_hint(&self.cursor)
    }
}

#[derive(Debug)]
pub struct CursorKeyRangeIter<'a> {
    start_key: MdbValue<'a>,
    end_key: MdbValue<'a>,
    end_inclusive: bool,
    marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> CursorKeyRangeIter<'a> {
    pub fn new<K: ToMdbValue+'a>(start_key: &'a K, end_key: &'a K, end_inclusive: bool) -> CursorKeyRangeIter<'a> {
        CursorKeyRangeIter {
            start_key: start_key.to_mdb_value(),
            end_key: end_key.to_mdb_value(),
            end_inclusive,
            marker: ::std::marker::PhantomData,
        }
    }
}

impl<'iter> IterateCursor for CursorKeyRangeIter<'iter> {
    fn init_cursor<'a, 'b: 'a, 'txn>(&'a self, cursor: & mut Cursor<'b, 'txn>) -> bool {
        let ok = unsafe {
            cursor.move_to_gte_key(mem::transmute::<&'a MdbValue<'a>, &'b MdbValue<'b>>(&self.start_key)).is_ok()
        };
        ok && cursor.cmp_key(&self.end_key).is_less(self.end_inclusive)
    }

    fn move_to_next<'i, 'c: 'i, 'txn>(&'i self, cursor: &'c mut Cursor<'c, 'txn>) -> bool {
        let moved = cursor.move_to_next_key().is_ok();
        if !moved {
            false
        } else {
            cursor.cmp_key(&self.end_key).is_less(self.end_inclusive)
        }
    }
}

#[derive(Debug)]
pub struct CursorFromKeyIter<'a> {
    start_key: MdbValue<'a>,
    marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> CursorFromKeyIter<'a> {
    pub fn new<K: ToMdbValue+'a>(start_key: &'a K) -> CursorFromKeyIter<'a> {
        CursorFromKeyIter {
            start_key: start_key.to_mdb_value(),
            marker: ::std::marker::PhantomData
        }
    }
}

impl<'iter> IterateCursor for CursorFromKeyIter<'iter> {
    fn init_cursor<'a, 'b: 'a, 'txn>(&'a self, cursor: & mut Cursor<'b, 'txn>) -> bool {
        unsafe {
            cursor.move_to_gte_key(mem::transmute::<&'a MdbValue<'a>, &'b MdbValue<'b>>(&self.start_key)).is_ok()
        }
    }

    fn move_to_next<'i, 'c: 'i, 'txn>(&'i self, cursor: &'c mut Cursor<'c, 'txn>) -> bool {
        cursor.move_to_next_key().is_ok()
    }
}


#[derive(Debug)]
pub struct CursorToKeyIter<'a> {
    end_key: MdbValue<'a>,
    marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> CursorToKeyIter<'a> {
    pub fn new<K: ToMdbValue+'a>(end_key: &'a K) -> CursorToKeyIter<'a> {
        CursorToKeyIter {
            end_key: end_key.to_mdb_value(),
            marker: ::std::marker::PhantomData,
        }
    }
}

impl<'iter> IterateCursor for CursorToKeyIter<'iter> {
    fn init_cursor<'a, 'b: 'a, 'txn>(&'a self, cursor: & mut Cursor<'b, 'txn>) -> bool {
        let ok = cursor.move_to_first().is_ok();
        ok && cursor.cmp_key(&self.end_key).is_less(false)
    }

    fn move_to_next<'i, 'c: 'i, 'txn>(&'i self, cursor: &'c mut Cursor<'c, 'txn>) -> bool {
        let moved = cursor.move_to_next_key().is_ok();
        if !moved {
            false
        } else {
            cursor.cmp_key(&self.end_key).is_less(false)
        }
    }
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub struct CursorIter;

impl<'iter> IterateCursor for CursorIter {
    fn init_cursor<'a, 'b: 'a, 'txn>(&'a self, cursor: & mut Cursor<'b, 'txn>) -> bool {
        cursor.move_to_first().is_ok()
    }

    fn move_to_next<'i, 'c: 'i, 'txn>(&'i self, cursor: &'c mut Cursor<'c, 'txn>) -> bool {
        cursor.move_to_next_key().is_ok()
    }
}

#[derive(Debug)]
pub struct CursorItemIter<'a> {
    key: MdbValue<'a>,
    marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> CursorItemIter<'a> {
    pub fn new<K: ToMdbValue+'a>(key: &'a K) -> CursorItemIter<'a> {
        CursorItemIter {
            key: key.to_mdb_value(),
            marker: ::std::marker::PhantomData
        }
    }
}

impl<'iter> IterateCursor for CursorItemIter<'iter> {
    fn init_cursor<'a, 'b: 'a, 'txn>(&'a self, cursor: & mut Cursor<'b, 'txn>) -> bool {
        unsafe {
            cursor.move_to_key(mem::transmute::<&MdbValue, &'b MdbValue<'b>>(&self.key)).is_ok()
        }
    }

    fn move_to_next<'i, 'c: 'i, 'txn>(&'i self, cursor: &'c mut Cursor<'c, 'txn>) -> bool {
        cursor.move_to_next_item().is_ok()
    }

    fn get_size_hint(&self, c: &Cursor) -> (usize, Option<usize>) {
        match c.item_count() {
            Err(_) => (0, None),
            Ok(cnt) => (0, Some(cnt as usize))
        }
    }
}