use std::env;
use std::fs::{self};
use std::path::{PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Once, ONCE_INIT};
use std::thread;

use libc::c_int;

use crate::core::{MdbValue, KeyExists, MdbError};
use crate::environment::{self, EnvBuilder, ENV_NO_MEM_INIT, ENV_NO_META_SYNC };
use crate::database::{self, DbFlags};
use ffi::MDB_val;
use crate::traits::FromMdbValue;

const USER_DIR: u32 = 0o777;
static TEST_ROOT_DIR: &'static str = "test-dbs";
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
static INIT_DIR_ONCE: Once = ONCE_INIT;

fn global_root() -> PathBuf {
     let mut path = env::current_exe().unwrap();
     path.pop(); // chop off exe name
     path.pop(); // chop off 'debug'

     // If `cargo test` is run manually then our path looks like
     // `target/debug/foo`, in which case our `path` is already pointing at
     // `target`. If, however, `cargo test --target $target` is used then the
     // output is `target/$target/debug/foo`, so our path is pointing at
     // `target/$target`. Here we conditionally pop the `$target` name.
     if path.file_name().and_then(|s| s.to_str()) != Some("target") {
         path.pop();
     }

     path.join(TEST_ROOT_DIR)
 }

fn next_path() -> PathBuf {
    let root_dir = global_root();

    INIT_DIR_ONCE.call_once(|| {
        if let Ok(root_meta) = fs::metadata(root_dir.clone()) {
            if root_meta.is_dir() {
                let _ = fs::remove_dir_all(&root_dir);
            }
        }
        assert!(fs::create_dir_all(&root_dir).is_ok());
    });

    let cur_id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let res = root_dir.join(&format!("db-{}", cur_id));
    println!("Testing db in {}", res.display());
    res
}

#[test]
fn test_environment() {
    let mut env = EnvBuilder::new()
        .max_readers(33)
        .open(&next_path(), USER_DIR).unwrap();

    env.sync(true).unwrap();

    let test_flags = ENV_NO_MEM_INIT | ENV_NO_META_SYNC;

    env.set_flags(test_flags, true).unwrap();
    let new_flags = env.get_flags().unwrap();
    assert!((new_flags & test_flags) == test_flags, "Get flags != set flags");

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();

    let key = "hello";
    let value = "world";

    db.set(&key, &value, &txn).unwrap();

    let v = db.get::<&str>(&key, &txn).unwrap();
    assert!(v == value, "Written {} and read {}", &value, &v);
}

#[test]
fn test_single_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1, &txn).is_ok());
    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.set(&test_key1, &test_data2, &txn).is_ok());
    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data2, "Data written differs from data read");

    assert!(db.del(&test_key1, &txn).is_ok());
    assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key should be deleted");
}

#[test]
fn test_multiple_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_ALLOW_DUPS).unwrap();
    {
        let txn = env.new_transaction().unwrap();

        let test_key1 = "key1";
        let test_data1 = "value1";
        let test_data2 = "value2";

        assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key shouldn't exist yet");

        assert!(db.set(&test_key1, &test_data1, &txn).is_ok());
        let v = db.get::<&str>(&test_key1, &txn).unwrap();
        assert!(v == test_data1, "Data written differs from data read");

        assert!(db.set(&test_key1, &test_data2, &txn).is_ok());
        let v = db.get::<&str>(&test_key1, &txn).unwrap();
        assert!(v == test_data1, "It should still return first value");

        assert!(db.del_item(&test_key1, &test_data1, &txn).is_ok());

        let v = db.get::<&str>(&test_key1, &txn).unwrap();
        assert!(v == test_data2, "It should return second value");
        assert!(db.del(&test_key1, &txn).is_ok());

        assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key shouldn't exist anymore!");
    }
}

#[test]
fn test_append_duplicate() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_ALLOW_DUPS).unwrap();
    {let txn = env.new_transaction().unwrap();
    //let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.append(&test_key1, &test_data1, &txn).is_ok());
    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.append_duplicate(&test_key1, &test_data2, &txn).is_ok());
    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data1, "It should still return first value");

    assert!(db.del_item(&test_key1, &test_data1, &txn).is_ok());

    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data2, "It should return second value");

    match db.append_duplicate(&test_key1, &test_data1, &txn).err().unwrap() {
        KeyExists => (),
        _ => panic!("Expected KeyExists error")
    }
    }
}

#[test]
fn test_insert_values() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    //let db = env.get_default_db(DbFlags::empty()).unwrap();
    let db = env.get_default_db(DbFlags::empty()).unwrap();
    {let txn = env.new_transaction().unwrap();

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1, &txn).is_ok());
    let v = db.get::<&str>(&test_key1, &txn).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.insert(&test_key1, &test_data2, &txn).is_err(), "Inserting should fail if key exists");

    assert!(db.del(&test_key1, &txn).is_ok());
    assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key should be deleted");

    assert!(db.insert(&test_key1, &test_data2, &txn).is_ok(), "Inserting should succeed");}
}

#[test]
fn test_resize_map() {
    use ffi::MDB_MAP_FULL;
    
    let env = EnvBuilder::new()
        .max_dbs(5)
        .map_size(0x1000u64)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();

    let mut key_idx = 0u64;
    let test_data: [u8; 0xFF] = [0x5A; 0xFF];

    let mut write_closure = || {
        let txn = env.new_transaction().unwrap();
        {
            let test_key = format!("key_{}", key_idx);
            match db.set(&test_key, &(&test_data[..]), &txn) {
                Ok(_) => (),
                Err(e) => return Err(e)
            }
        }
        key_idx += 1;
        txn.commit()
    };
    // write data until running into 'MDB_MAP_FULL' error
    loop {
        match write_closure() {
            Err(MdbError::Other(MDB_MAP_FULL, _)) => { break; }
            Err(e) => panic!("unexpected db error {}", e),
            _ => {} // continue
        }
    }

    // env should be still ok and resizable
    assert!(env.set_mapsize(0x100000usize).is_ok(), "Couldn't resize map");

    // next write after resize should not fail
    let txn = env.new_transaction().unwrap();
    {
        let test_key = "different_key";
        assert!(db.set(&test_key, &(&test_data[..]), &txn).is_ok(), "set after resize failed");
    }
    assert!(txn.commit().is_ok(), "Commit failed after resizing map");
}

#[test]
fn test_stat() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    // ~ the two dataset; each to end up in its own database
    let dss = [
        // ~ keep the "default db" dataset here at the beginning (see
        // the assertion at the end of this test)
        ("", vec![("default", "db"), ("has", "some"), ("extras", "prepared")]),
        ("db1", vec![("foo", "bar"), ("quux", "qak")]),
        ("db2", vec![("a", "abc"), ("b", "bcd"), ("c", "cde"), ("d", "def")]),
        ("db3", vec![("hip", "hop")])];

    // ~ create each db, populate it, and assert db.stat() for each seperately
    for &(name, ref ds) in &dss {
        let db = env.create_db(name, DbFlags::empty()).unwrap();
        let tx = env.new_transaction().unwrap();
        {
            for &(k, v) in ds {
                assert!(db.set(&k, &v, &tx).is_ok());
            }
            // ~ verify the expected number of entries (key/value pairs) in the db
            let stat = db.stat(&tx).unwrap();
            assert_eq!(ds.len() as usize, stat.ms_entries);
        }
        tx.commit().unwrap();
    }

    // ~ now verify the number of data items in this _environment_ (this
    // is the number key/value pairs in the default database plus the
    // number of other databases)
    let stat = env.stat().unwrap();
    assert_eq!(dss[0].1.len() + dss[1..].len(), stat.ms_entries);
}

#[test]
fn test_cursor_le() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_INT_KEY).unwrap();
    let txn = env.new_transaction().unwrap();

    let mut search_key = 10;
    let mut cursor = db.new_cursor(&txn).unwrap();

    if let Err(e) = cursor.move_to_lte_key(&search_key) {
        println!("{:?}", e);
    }
    let test_key1 = 10;
    let test_key2 = 20;
    let val1="one";
    let val2="two";
    let _ = db.set(&test_key1, &val1, &txn);
    let _ = db.set(&test_key2, &val2, &txn);
    search_key = 15;

    assert!(cursor.move_to_lte_key(&search_key).is_ok());
    assert_eq!((10, "one"), cursor.get::<u32, &str>().unwrap());
    
    search_key = 20;
    assert!(cursor.move_to_lte_key(&search_key).is_ok());
    assert_eq!((20, "two"), cursor.get::<u32, &str>().unwrap());

    search_key = 25;
    assert!(cursor.move_to_lte_key(&search_key).is_ok());
    assert_eq!((20, "two"), cursor.get::<u32, &str>().unwrap());

}


#[test]
fn test_cursor_le_dup() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.create_db("test_le_dup", database::DB_INT_KEY | database::DB_ALLOW_DUPS | database::DB_ALLOW_INT_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();

    let mut cursor = db.new_cursor(&txn).unwrap();

    let test_key1 = 10;
    let test_key2 = 20;
    let key1_val1=101;
    let key1_val2=102;
    let key2_val1=201;
    let key2_val2=202;
    let _ = db.set(&test_key1, &key1_val1, &txn);
    let _ = db.set(&test_key1, &key1_val2, &txn);
    let _ = db.set(&test_key2, &key2_val1, &txn);
    let _ = db.set(&test_key2, &key2_val2, &txn);
    let mut search_key = 15;
    assert!(cursor.move_to_lte_key_first_item(&search_key).is_ok());
    assert_eq!((10, 101), cursor.get::<u32, u32>().unwrap());
    
    search_key = 20;
    assert!(cursor.move_to_lte_key_first_item(&search_key).is_ok());
    assert_eq!((20, 201), cursor.get::<u32, u32>().unwrap());

    search_key = 25;
    assert!(cursor.move_to_lte_key_first_item(&search_key).is_ok());
    assert_eq!((20, 201), cursor.get::<u32, u32>().unwrap());

    search_key = 10;
    let mut dup_key = 102;
    assert!(cursor.move_to_lte_key_and_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 102), cursor.get::<u32, u32>().unwrap());

    search_key = 10;
    dup_key = 103;
    assert!(cursor.move_to_lte_key_and_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 102), cursor.get::<u32, u32>().unwrap());

    search_key = 12;
    dup_key = 103;
    assert!(cursor.move_to_lte_key_and_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 102), cursor.get::<u32, u32>().unwrap());

    search_key = 12;
    dup_key = 102;
    assert!(cursor.move_to_lte_key_and_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 102), cursor.get::<u32, u32>().unwrap());

}

#[test]
fn test_cursor_ge_dup() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.create_db("test_ge_dup", database::DB_INT_KEY | database::DB_ALLOW_DUPS | database::DB_ALLOW_INT_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();

    let mut cursor = db.new_cursor(&txn).unwrap();

    let test_key1 = 10;
    let test_key2 = 20;
    let key1_val1=100;
    let key1_val2=110;
    let key2_val1=200;
    let key2_val2=210;
    let _ = db.set(&test_key1, &key1_val1, &txn);
    let _ = db.set(&test_key1, &key1_val2, &txn);
    let _ = db.set(&test_key2, &key2_val1, &txn);
    let _ = db.set(&test_key2, &key2_val2, &txn);
    
    let mut search_key = 10;
    let mut dup_key = 99;
    assert!(cursor.move_to_gte_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 100), cursor.get::<u32, u32>().unwrap());
    
    search_key = 10;
    dup_key = 105;
    assert!(cursor.move_to_gte_item(&search_key, &dup_key).is_ok());
    assert_eq!((10, 110), cursor.get::<u32, u32>().unwrap());

    search_key = 20;
    assert!(cursor.move_to_gte_item(&search_key, &dup_key).is_ok());
    assert_eq!((20, 200), cursor.get::<u32, u32>().unwrap());

    search_key = 20;
    dup_key = 205;
    assert!(cursor.move_to_lte_item(&search_key, &dup_key).is_ok());
    assert_eq!((20, 200), cursor.get::<u32, u32>().unwrap());
}


#[test]
fn test_cursors() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_ALLOW_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();

    let test_key1 = "key1";
    let test_key2 = "key2";
    let test_values: Vec<&str> = vec!("value1", "value2", "value3", "value4");

    assert!(db.get::<()>(&test_key1, &txn).is_err(), "Key shouldn't exist yet");

    for t in test_values.iter() {
        let _ = db.set(&test_key1, t, &txn);
        let _ = db.set(&test_key2, t, &txn);
    }

    let mut cursor = db.new_cursor(&txn).unwrap();
    assert!(cursor.move_to_first().is_ok());

    assert!(cursor.move_to_key(&test_key1).is_ok());
    assert!(cursor.item_count().unwrap() == 4);

    assert!(cursor.del_item().is_ok());
    assert!(cursor.item_count().unwrap() == 3);

    assert!(cursor.move_to_key(&test_key1).is_ok());
    let new_value = "testme";

    assert!(cursor.replace(&new_value).is_ok());
    {
        let (_, v) = cursor.get::<(), &str>().unwrap();
        // NOTE: this asserting will work once new_value is
        // of the same length as it is inplace change
        assert!(v == new_value);
    }

    assert!(cursor.del_all().is_ok());
    assert!(cursor.move_to_key(&test_key1).is_err());

    assert!(cursor.move_to_key(&test_key2).is_ok());
}


#[test]
fn test_cursor_item_manip() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_ALLOW_DUPS | database::DB_ALLOW_INT_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();

    let test_key1 = "key1";

    assert!(db.set(&test_key1, &3u64, &txn).is_ok());

    let mut cursor = db.new_cursor(&txn).unwrap();
    assert!(cursor.move_to_key(&test_key1).is_ok());

    let values: Vec<u64> = db.item_iter(&test_key1, &txn).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();
    assert_eq!(values, vec![3u64]);

    assert!(cursor.add_item(&4u64).is_ok());
    assert!(cursor.add_item(&5u64).is_ok());

    let values: Vec<u64> = db.item_iter(&test_key1, &txn).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();
    assert_eq!(values, vec![3u64, 4, 5]);

    assert!(cursor.replace(&6u64).is_ok());
    let values: Vec<u64> = db.item_iter(&test_key1, &txn).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();

    assert_eq!(values, vec![3u64, 4, 6]);
}

fn as_slices(v: &Vec<String>) -> Vec<&str> {
    v.iter().map(|s| &s[..]).collect::<Vec<&str>>()
}

#[test]
fn test_item_iter() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(database::DB_ALLOW_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";
    let test_key2 = "key2";
    let test_key3 = "key3";

    assert!(db.set(&test_key1, &test_data1, &txn).is_ok());
    assert!(db.set(&test_key1, &test_data2, &txn).is_ok());
    assert!(db.set(&test_key2, &test_data1, &txn).is_ok());

    let iter = db.item_iter(&test_key1, &txn).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1, test_data2]);

    let iter = db.item_iter(&test_key2, &txn).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1]);

    let iter = db.item_iter(&test_key3, &txn).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(values.len(), 0);
}

#[test]
fn test_db_creation() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    assert!(env.create_db("test-db", DbFlags::empty()).is_ok());
}

#[test]
fn test_read_only_txn() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    env.get_reader().unwrap();
}

#[test]
fn test_cursor_in_txns() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    {
        let db = env.create_db("test1", database::DB_ALLOW_DUPS
 | database::DB_ALLOW_INT_DUPS).unwrap();
        let txn = env.new_transaction().unwrap();
        {
            let cursor = db.new_cursor(&txn);
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }

    {
        let db = env.create_db("test1", database::DB_ALLOW_DUPS
 | database::DB_ALLOW_INT_DUPS).unwrap();
        let txn = env.new_transaction().unwrap();
        {

            let cursor = db.new_cursor(&txn);
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }
}

#[test]
fn test_multithread_env() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let shared_env = env.clone();
    let key = "key";
    let value = "value";

    let _ = thread::spawn(move || {
        let db = shared_env.create_db("test1", DbFlags::empty()).unwrap();
        let txn = shared_env.new_transaction().unwrap();
        {
            assert!(db.set(&key, &value, &txn).is_ok());
        }
        assert!(txn.commit().is_ok());
    }).join();

    let db = env.create_db("test1", DbFlags::empty()).unwrap();
    let txn = env.get_reader().unwrap();
    let value2: &str = db.get(&key, &txn).unwrap();
    assert_eq!(value, value2);
}

#[test]
fn test_keyrange_to() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();
    let keys:   Vec<u64> = vec![1, 2, 3];
    let values: Vec<u64> = vec![5, 6, 7];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {

        let last_idx = keys.len() - 1;
        let last_key: u64 = keys[last_idx];
        // last key is excluded
        let iter = db.keyrange_to(&last_key, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &values[..last_idx]);
    }
}

/// Test that selecting a key range with an upper bound smaller than
/// the smallest key in the db yields an empty range.
#[test]
fn test_keyrange_to_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 50), (11, 60), (12, 70)];

    let txn = env.new_transaction().unwrap();
    {
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {

        // last key is excluded
        let upper_bound: u64 = 1;
        let iter = db.keyrange_to(&upper_bound, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }
}

#[test]
fn test_keyrange_from() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();
    let keys:   Vec<u64> = vec![1, 2, 3];
    let values: Vec<u64> = vec![5, 6, 7];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {

        let start_idx = 1; // second key
        let last_key: u64 = keys[start_idx];
        let iter = db.keyrange_from(&last_key, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &values[start_idx..]);
    }
}

/// Test that selecting a key range with a lower bound greater than
/// the biggest key in the db yields an empty range.
#[test]
fn test_keyrange_from_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 50), (11, 60), (12, 70)];

    let txn = env.new_transaction().unwrap();
    {
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {

        // last key is excluded
        let lower_bound = recs[recs.len()-1].0 + 1;
        let iter = db.keyrange_from(&lower_bound, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }
}

#[test]
fn test_keyrange() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_ALLOW_DUPS | database::DB_INT_KEY).unwrap();
    let keys:   Vec<u64> = vec![ 1,  2,  3,  4,  5,  6];
    let values: Vec<u64> = vec![10, 11, 12, 13, 14, 15];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange(&keys[start_idx], &keys[end_idx], &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();

         //  +1 as Rust slices do not include end
        assert_eq!(res, &values[start_idx.. end_idx + 1]);
    }
}

/// Test that select a key range outside the available data correctly
/// yields an empty range.
#[test]
fn test_keyrange_init_cursor() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_ALLOW_DUPS | database::DB_INT_KEY).unwrap();
    let keys:   Vec<u64> = vec![ 1,  2,  3,  4,  5,  6];
    let values: Vec<u64> = vec![10, 11, 12, 13, 14, 15];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    // test the cursor initialization before the available data range
    let txn = env.get_reader().unwrap();
    {
        let start_key = 0u64;
        let end_key = 0u64;
        let iter = db.keyrange(&start_key, &end_key, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }

    // test the cursor initialization after the available data range
    {
        let start_key = 10;
        let end_key = 20;
        let iter = db.keyrange(&start_key, &end_key, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert!(res.is_empty());
    }
}

#[test]
fn test_keyrange_from_to() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_ALLOW_DUPS | database::DB_INT_KEY).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 11), (20, 21), (30, 31), (40, 41), (50, 51)];

    let txn = env.new_transaction().unwrap();
    {
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v, &txn).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange_from_to(&recs[start_idx].0, &recs[end_idx].0, &txn).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        // ~ end_key must be excluded here
        let exp: Vec<_> = recs[start_idx .. end_idx].iter().map(|x| x.1).collect();
        assert_eq!(res, exp);
    }
}

#[test]
fn test_readonly_env() {
    let recs: Vec<(u32,u32)> = vec![(10, 11), (11, 12), (12, 13), (13,14)];

    // ~ first create a new read-write environment with its default
    // database containing a few entries
    let path = next_path();
    {
        let rw_env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
        let dbh = rw_env.get_default_db(database::DB_INT_KEY).unwrap();
        let tx = rw_env.new_transaction().unwrap();
        {
            for &rec in recs.iter() {
                dbh.set(&rec.0, &rec.1, &tx).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    // ~ now re-open the previously created database in read-only mode
    // and iterate the key/value pairs
    let ro_env = EnvBuilder::new()
        .flags(environment::ENV_CREATE_READONLY)
        .open(&path, USER_DIR).unwrap();
    let dbh = ro_env.get_default_db(database::DB_INT_KEY).unwrap();
    assert!(ro_env.new_transaction().is_err());
    let mut tx = ro_env.get_reader().unwrap();
    {
        let kvs: Vec<(u32,u32)> = dbh.iter(&tx).unwrap().map(|c| c.get()).collect();
        assert_eq!(recs, kvs);
    }
    tx.abort();
}

unsafe fn negative_if_odd_i32_val(val: *const MDB_val) -> i32 {
    let v = MdbValue::from_raw(val);
    let i = i32::from_mdb_value(&v);
    if i % 2 == 0 {
        i
    } else {
        -i
    }
}

// A nonsensical comparison function that sorts differently that byte-by-byte comparison
extern "C" fn negative_odd_cmp_fn(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    unsafe {
        let lhs = negative_if_odd_i32_val(lhs_val);
        let rhs = negative_if_odd_i32_val(rhs_val);
        lhs - rhs
    }
}

#[test]
fn test_compare() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let val: i32 = 0;
    {
        assert!(db.set_compare(negative_odd_cmp_fn, &txn).is_ok());

        let i: i32 = 2;
        db.set(&i, &val, &txn).unwrap();
        let i: i32 = 3;
        db.set(&i, &val, &txn).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let i: i32 = 4;
        db.set(&i, &val, &txn).unwrap();
        let i: i32 = 5;
        db.set(&i, &val, &txn).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let keys: Vec<_> = db.iter(&txn).unwrap().map(|cv| cv.get_key::<i32>()).collect();
        assert_eq!(keys, [5, 3, 2, 4]);
    }
    assert!(txn.commit().is_ok());
}

#[test]
fn test_dupsort() {
    let env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_ALLOW_DUPS).unwrap();
    let txn = env.new_transaction().unwrap();
    let key: i32 = 0;
    {
        assert!(db.set_dupsort(negative_odd_cmp_fn, &txn).is_ok());

        let i: i32 = 2;
        db.set(&key, &i, &txn).unwrap();
        let i: i32 = 3;
        db.set(&key, &i, &txn).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let i: i32 = 4;
        db.set(&key, &i, &txn).unwrap();
        let i: i32 = 5;
        db.set(&key, &i, &txn).unwrap();
    }
    assert!(txn.commit().is_ok());

    let txn = env.new_transaction().unwrap();
    {
        let vals: Vec<_> = db.item_iter(&key, &txn).unwrap().map(|cv| cv.get_value::<i32>()).collect();
        assert_eq!(vals, [5, 3, 2, 4]);
    }
    assert!(txn.commit().is_ok());
}

// ~ see #29
#[test]
fn test_conversion_to_vecu8() {
    let rec: (u32, Vec<u8>) = (10, vec![1,2,3,4,5]);

    let path = next_path();
    let env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();

    // ~ add our test record
    {
        let tx = env.new_transaction().unwrap();
        {
            db.set(&rec.0, &rec.1, &tx).unwrap();
        }
        tx.commit().unwrap();
    }

    // ~ validate the behavior
    let tx = env.new_transaction().unwrap();
    {
        {
            // ~ now retrieve a Vec<u8> and make sure it is dropped
            // earlier than our database handle
            let xs: Vec<u8> = db.get(&rec.0, &tx).unwrap();
            assert_eq!(rec.1, xs);
        }
    }
    tx.abort();
}

// ~ see #29
#[test]
fn test_conversion_to_string() {
    let rec: (u32, String) = (10, "hello, world".to_owned());

    let path = next_path();
    let env = EnvBuilder::new().open(&path, USER_DIR).unwrap();
    let db = env.get_default_db(database::DB_INT_KEY).unwrap();

    // ~ add our test record
    {
        let tx = env.new_transaction().unwrap();
        {
            db.set(&rec.0, &rec.1, &tx).unwrap();
        }
        tx.commit().unwrap();
    }

    // ~ validate the behavior
    {
    let tx = env.new_transaction().unwrap();
        {
            // ~ now retrieve a String and make sure it is dropped
            // earlier than our database handle
            let xs: String = db.get(&rec.0, &tx).unwrap();
            assert_eq!(rec.1, xs);
        }
    tx.abort();
    }
}

/*
#[test]
fn test_compilation_of_moved_items() {
    let path = Path::new("dbcom");
    test_db_in_path(&next_path(), || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&next_path(), USER_DIR)
            .unwrap();

        let db = env.get_default_db(DbFlags::empty()).unwrap();
        let mut txn = env.new_transaction().unwrap();

        txn.commit();

        let test_key1 = "key1";
        let test_data1 = "value1";

        assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist yet"); // ~ERROR: use of moved value
    })
}
*/
