use libc::{c_int, c_uint, size_t};
use std;
use std::borrow::ToOwned;
use std::cell::{UnsafeCell};
use std::collections::HashMap;
use std::ffi::{CString};
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use ffi::{self};

use crate::core::{ MdbError, MdbResult };
use crate::database::Database;
use crate::transaction::{ NativeTransaction, Transaction, ReadonlyTransaction };
use crate::database::{ DbFlags, DB_CREATE };

bitflags! {
    #[doc = "A set of environment flags which could be changed after opening"]

    pub flags EnvFlags: c_uint {

        #[doc="Don't flush system buffers to disk when committing a
        transaction. This optimization means a system crash can
        corrupt the database or lose the last transactions if buffers
        are not yet flushed to disk. The risk is governed by how
        often the system flushes dirty buffers to disk and how often
        mdb_env_sync() is called. However, if the filesystem
        preserves write order and the MDB_WRITEMAP flag is not used,
        transactions exhibit ACI (atomicity, consistency, isolation)
        properties and only lose D (durability). I.e. database
        integrity is maintained, but a system crash may undo the
        final transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP)
        leaves the system with no hint for when to write transactions
        to disk, unless mdb_env_sync() is called. (MDB_MAPASYNC |
        MDB_WRITEMAP) may be preferable. This flag may be changed at
        any time using mdb_env_set_flags()."]
        const ENV_NO_SYNC      = ffi::MDB_NOSYNC,

        #[doc="Flush system buffers to disk only once per transaction,
        omit the metadata flush. Defer that until the system flushes
        files to disk, or next non-MDB_RDONLY commit or
        mdb_env_sync(). This optimization maintains database
        integrity, but a system crash may undo the last committed
        transaction. I.e. it preserves the ACI (atomicity,
        consistency, isolation) but not D (durability) database
        property. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const ENV_NO_META_SYNC  = ffi::MDB_NOMETASYNC,

        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to
        disk. As with MDB_NOSYNC, a system crash can then corrupt the
        database or lose the last transactions. Calling
        mdb_env_sync() ensures on-disk database integrity until next
        commit. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const ENV_MAP_ASYNC    = ffi::MDB_MAPASYNC,

        #[doc="Don't initialize malloc'd memory before writing to
        unused spaces in the data file. By default, memory for pages
        written to the data file is obtained using malloc. While
        these pages may be reused in subsequent transactions, freshly
        malloc'd pages will be initialized to zeroes before use. This
        avoids persisting leftover data from other code (that used
        the heap and subsequently freed the memory) into the data
        file. Note that many other system libraries may allocate and
        free memory from the heap for arbitrary uses. E.g., stdio may
        use the heap for file I/O buffers. This initialization step
        has a modest performance cost so some applications may want
        to disable it using this flag. This option can be a problem
        for applications which handle sensitive data like passwords,
        and it makes memory checkers like Valgrind noisy. This flag
        is not needed with MDB_WRITEMAP, which writes directly to the
        mmap instead of using malloc for pages. The initialization is
        also skipped if MDB_RESERVE is used; the caller is expected
        to overwrite all of the memory that was reserved in that
        case. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const ENV_NO_MEM_INIT   = ffi::MDB_NOMEMINIT
    }
}

bitflags! {
    #[doc = "A set of all environment flags"]

    pub flags EnvCreateFlags: c_uint {
        #[doc="Use a fixed address for the mmap region. This flag must be"]
        #[doc=" specified when creating the environment, and is stored persistently"]
        #[doc=" in the environment. If successful, the memory map will always reside"]
        #[doc=" at the same virtual address and pointers used to reference data items"]
        #[doc=" in the database will be constant across multiple invocations. This "]
        #[doc="option may not always work, depending on how the operating system has"]
        #[doc=" allocated memory to shared libraries and other uses. The feature is highly experimental."]
        const ENV_CREATE_FIXED_MAP    = ffi::MDB_FIXEDMAP,
        #[doc="By default, LMDB creates its environment in a directory whose"]
        #[doc=" pathname is given in path, and creates its data and lock files"]
        #[doc=" under that directory. With this option, path is used as-is"]
        #[doc=" for the database main data file. The database lock file is"]
        #[doc=" the path with \"-lock\" appended."]
        const ENV_CREATE_NO_SUB_DIR    = ffi::MDB_NOSUBDIR,
        #[doc="Don't flush system buffers to disk when committing a"]
        #[doc=" transaction. This optimization means a system crash can corrupt"]
        #[doc=" the database or lose the last transactions if buffers are not"]
        #[doc=" yet flushed to disk. The risk is governed by how often the"]
        #[doc=" system flushes dirty buffers to disk and how often"]
        #[doc=" mdb_env_sync() is called. However, if the filesystem preserves"]
        #[doc=" write order and the MDB_WRITEMAP flag is not used, transactions"]
        #[doc=" exhibit ACI (atomicity, consistency, isolation) properties and"]
        #[doc=" only lose D (durability). I.e. database integrity is"]
        #[doc=" maintained, but a system crash may undo the final"]
        #[doc=" transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves"]
        #[doc=" the system with no hint for when to write transactions to"]
        #[doc=" disk, unless mdb_env_sync() is called."]
        #[doc=" (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This flag"]
        #[doc=" may be changed at any time using mdb_env_set_flags()."]
        const ENV_CREATE_NO_SYNC      = ffi::MDB_NOSYNC,
        #[doc="Open the environment in read-only mode. No write operations"]
        #[doc=" will be allowed. LMDB will still modify the lock file - except"]
        #[doc=" on read-only filesystems, where LMDB does not use locks."]
        const ENV_CREATE_READONLY    = ffi::MDB_RDONLY,
        #[doc="Flush system buffers to disk only once per transaction,"]
        #[doc=" omit the metadata flush. Defer that until the system flushes"]
        #[doc=" files to disk, or next non-MDB_RDONLY commit or mdb_env_sync()."]
        #[doc=" This optimization maintains database integrity, but a system"]
        #[doc=" crash may undo the last committed transaction. I.e. it"]
        #[doc=" preserves the ACI (atomicity, consistency, isolation) but"]
        #[doc=" not D (durability) database property. This flag may be changed"]
        #[doc=" at any time using mdb_env_set_flags()."]
        const ENV_CREATE_NO_META_SYNC  = ffi::MDB_NOMETASYNC,
        #[doc="Use a writeable memory map unless MDB_RDONLY is set. This is"]
        #[doc="faster and uses fewer mallocs, but loses protection from"]
        #[doc="application bugs like wild pointer writes and other bad updates"]
        #[doc="into the database. Incompatible with nested"]
        #[doc="transactions. Processes with and without MDB_WRITEMAP on the"]
        #[doc="same environment do not cooperate well."]
        const ENV_CREATE_WRITE_MAP    = ffi::MDB_WRITEMAP,
        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to disk. As"]
        #[doc="with MDB_NOSYNC, a system crash can then corrupt the database or"]
        #[doc="lose the last transactions. Calling mdb_env_sync() ensures"]
        #[doc="on-disk database integrity until next commit. This flag may be"]
        #[doc="changed at any time using mdb_env_set_flags()."]
        const ENV_CREATE_MAP_ASYNC    = ffi::MDB_MAPASYNC,
        #[doc="Don't use Thread-Local Storage. Tie reader locktable slots to"]
        #[doc="ffi::MDB_txn objects instead of to threads. I.e. mdb_txn_reset()"]
        #[doc="keeps the slot reseved for the ffi::MDB_txn object. A thread may"]
        #[doc="use parallel read-only transactions. A read-only transaction may"]
        #[doc="span threads if the user synchronizes its use. Applications that"]
        #[doc="multiplex many user threads over individual OS threads need this"]
        #[doc="option. Such an application must also serialize the write"]
        #[doc="transactions in an OS thread, since LMDB's write locking is"]
        #[doc="unaware of the user threads."]
        const ENV_CREATE_NO_TLS       = ffi::MDB_NOTLS,
        #[doc="Don't do any locking. If concurrent access is anticipated, the"]
        #[doc="caller must manage all concurrency itself. For proper operation"]
        #[doc="the caller must enforce single-writer semantics, and must ensure"]
        #[doc="that no readers are using old transactions while a writer is"]
        #[doc="active. The simplest approach is to use an exclusive lock so"]
        #[doc="that no readers may be active at all when a writer begins. "]
        const ENV_CREATE_NO_LOCK      = ffi::MDB_NOLOCK,
        #[doc="Turn off readahead. Most operating systems perform readahead on"]
        #[doc="read requests by default. This option turns it off if the OS"]
        #[doc="supports it. Turning it off may help random read performance"]
        #[doc="when the DB is larger than RAM and system RAM is full. The"]
        #[doc="option is not implemented on Windows."]
        const ENV_CREATE_NO_READ_AHEAD = ffi::MDB_NORDAHEAD,
        #[doc="Don't initialize malloc'd memory before writing to unused spaces"]
        #[doc="in the data file. By default, memory for pages written to the"]
        #[doc="data file is obtained using malloc. While these pages may be"]
        #[doc="reused in subsequent transactions, freshly malloc'd pages will"]
        #[doc="be initialized to zeroes before use. This avoids persisting"]
        #[doc="leftover data from other code (that used the heap and"]
        #[doc="subsequently freed the memory) into the data file. Note that"]
        #[doc="many other system libraries may allocate and free memory from"]
        #[doc="the heap for arbitrary uses. E.g., stdio may use the heap for"]
        #[doc="file I/O buffers. This initialization step has a modest"]
        #[doc="performance cost so some applications may want to disable it"]
        #[doc="using this flag. This option can be a problem for applications"]
        #[doc="which handle sensitive data like passwords, and it makes memory"]
        #[doc="checkers like Valgrind noisy. This flag is not needed with"]
        #[doc="MDB_WRITEMAP, which writes directly to the mmap instead of using"]
        #[doc="malloc for pages. The initialization is also skipped if"]
        #[doc="MDB_RESERVE is used; the caller is expected to overwrite all of"]
        #[doc="the memory that was reserved in that case. This flag may be"]
        #[doc="changed at any time using mdb_env_set_flags()."]
        const ENV_CREATE_NO_MEM_INIT   = ffi::MDB_NOMEMINIT
    }
}

/// Constructs environment with settigs which couldn't be
/// changed after opening. By default it tries to create
/// corresponding dir if it doesn't exist, use `autocreate_dir()`
/// to override that behavior
#[derive(Clone, Debug)]
pub struct EnvBuilder {
    flags: EnvCreateFlags,
    max_readers: Option<usize>,
    max_dbs: Option<usize>,
    map_size: Option<u64>,
    autocreate_dir: bool,
}

impl EnvBuilder {
    pub fn new() -> EnvBuilder {
        EnvBuilder {
            flags: EnvCreateFlags::empty(),
            max_readers: None,
            max_dbs: None,
            map_size: None,
            autocreate_dir: true,
        }
    }

    /// Sets environment flags
    pub fn flags(mut self, flags: EnvCreateFlags) -> EnvBuilder {
        self.flags = flags;
        self
    }

    /// Sets max concurrent readers operating on environment
    pub fn max_readers(mut self, max_readers: usize) -> EnvBuilder {
        self.max_readers = Some(max_readers);
        self
    }

    /// Set max number of databases
    pub fn max_dbs(mut self, max_dbs: usize) -> EnvBuilder {
        self.max_dbs = Some(max_dbs);
        self
    }

    /// Sets max environment size, i.e. size in memory/disk of
    /// all data
    pub fn map_size(mut self, map_size: u64) -> EnvBuilder {
        self.map_size = Some(map_size);
        self
    }

    /// Sets whetever `lmdb-rs` should try to autocreate dir with default
    /// permissions on opening (default is true)
    pub fn autocreate_dir(mut self, autocreate_dir: bool)  -> EnvBuilder {
        self.autocreate_dir = autocreate_dir;
        self
    }

    /// Opens environment in specified path
    pub fn open<P: AsRef<Path>>(self, path: P, perms: u32) -> MdbResult<Environment> {
        let changeable_flags: EnvCreateFlags = ENV_CREATE_MAP_ASYNC | ENV_CREATE_NO_MEM_INIT | ENV_CREATE_NO_SYNC | ENV_CREATE_NO_META_SYNC;

        let env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            let p_env: *mut *mut ffi::MDB_env = &env as *const *mut ffi::MDB_env as *mut *mut ffi::MDB_env;
            try_mdb!(ffi::mdb_env_create(p_env));
        }

        // Enable only flags which can be changed, otherwise it'll fail
        try_mdb!(unsafe { ffi::mdb_env_set_flags(env, self.flags.bits() & changeable_flags.bits(), 1)});

        if let Some(map_size) = self.map_size {
            try_mdb!(unsafe { ffi::mdb_env_set_mapsize(env, map_size as size_t)});
        }

        if let Some(max_readers) = self.max_readers {
            try_mdb!(unsafe { ffi::mdb_env_set_maxreaders(env, max_readers as u32)});
        }

        if let Some(max_dbs) = self.max_dbs {
            try_mdb!(unsafe { ffi::mdb_env_set_maxdbs(env, max_dbs as u32)});
        }

        if self.autocreate_dir {
            EnvBuilder::check_path(&path, self.flags)?;
        }

        let is_readonly = self.flags.contains(ENV_CREATE_READONLY);

        let res = unsafe {
            // FIXME: revert back once `convert` is stable
            // let c_path = path.as_os_str().to_cstring().unwrap();
            let path_str = path.as_ref().to_str().ok_or(MdbError::InvalidPath)?;
            let c_path = CString::new(path_str).map_err(|_| MdbError::InvalidPath)?;

            ffi::mdb_env_open(env, c_path.as_ref().as_ptr(), self.flags.bits(),
                              perms as ffi::mdb_mode_t)
        };

        drop(self);
        match res {
            ffi::MDB_SUCCESS => {
                Ok(Environment::from_raw(env, is_readonly))
            },
            _ => {
                unsafe { ffi::mdb_env_close(env); }
                Err(MdbError::new_with_code(res))
            }
        }

    }

    fn check_path<P: AsRef<Path>>(path: P, flags: EnvCreateFlags) -> MdbResult<()> {
        use std::{fs, io};

        if flags.contains(ENV_CREATE_NO_SUB_DIR) {
            // FIXME: check parent dir existence/absence
            warn!("checking for path in NoSubDir mode isn't implemented yet");
            return Ok(());
        }

        // There should be a directory before open
        match fs::metadata(&path) {
            Ok(meta) => {
                if meta.is_dir() {
                    Ok(())
                } else {
                    Err(MdbError::InvalidPath)
                }
            },
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    fs::create_dir_all(path.as_ref()).map_err(|e| {
                        error!("failed to auto create dir: {}", e);
                        MdbError::InvalidPath
                    })
                } else {
                    Err(MdbError::InvalidPath)
                }
            }
        }
    }
}

#[derive(Debug)]
struct EnvHandle(*mut ffi::MDB_env);

impl Drop for EnvHandle {
    fn drop(&mut self) {
        unsafe {
            if self.0.is_null() {
                ffi::mdb_env_close(self.0);
            }
        }
    }
}

/// Represents LMDB Environment. Should be opened using `EnvBuilder`
#[derive(Debug)]
pub struct Environment {
    env: Arc<EnvHandle>,
    db_cache: Arc<Mutex<UnsafeCell<HashMap<String, ffi::MDB_dbi>>>>,
    is_readonly: bool, // true if opened in 'read-only' mode
}

impl Environment {

    pub fn new() -> EnvBuilder {
        EnvBuilder::new()
    }

    fn from_raw(env: *mut ffi::MDB_env, is_readonly: bool) -> Environment {
        Environment {
            env: Arc::new(EnvHandle(env)),
            db_cache: Arc::new(Mutex::new(UnsafeCell::new(HashMap::new()))),
            is_readonly,
        }
    }

    /// Check for stale entries in the reader lock table.
    ///
    /// Returns the number of stale slots that were cleared.
    pub fn reader_check(&self) -> MdbResult<c_int> {
        let mut dead: c_int = 0;
        lift_mdb!(unsafe { ffi::mdb_reader_check(self.env.0, &mut dead as *mut c_int)}, dead)
    }

    /// Retrieve environment statistics
    pub fn stat(&self) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_stat(self.env.0, &mut tmp)}, tmp)
    }

    pub fn info(&self) -> MdbResult<ffi::MDB_envinfo> {
        let mut tmp: ffi::MDB_envinfo = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_info(self.env.0, &mut tmp)}, tmp)
    }

    /// Sync environment to disk
    pub fn sync(&self, force: bool) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_sync(self.env.0, if force {1} else {0})})
    }

    /// Sets map size.
    /// This can be called after [open](struct.EnvBuilder.html#method.open) if no transactions are active in this process.
    pub fn set_mapsize(&self, map_size: usize) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_set_mapsize(self.env.0, map_size as size_t)})
    }

    /// This one sets only flags which are available for change even
    /// after opening, see also [get_flags](#method.get_flags) and [get_all_flags](#method.get_all_flags)
    pub fn set_flags(&mut self, flags: EnvFlags, turn_on: bool) -> MdbResult<()> {
        lift_mdb!(unsafe {
            ffi::mdb_env_set_flags(self.env.0, flags.bits(), if turn_on {1} else {0})
        })
    }

    /// Get flags of environment, which could be changed after it was opened
    /// use [get_all_flags](#method.get_all_flags) if you need also creation time flags
    pub fn get_flags(&self) -> MdbResult<EnvFlags> {
        let tmp = self.get_all_flags()?;
        Ok(EnvFlags::from_bits_truncate(tmp.bits()))
    }

    /// Get all flags of environment, including which were specified on creation
    /// See also [get_flags](#method.get_flags) if you're interested only in modifiable flags
    pub fn get_all_flags(&self) -> MdbResult<EnvCreateFlags> {
        let mut flags: c_uint = 0;
        lift_mdb!(unsafe {ffi::mdb_env_get_flags(self.env.0, &mut flags)}, EnvCreateFlags::from_bits_truncate(flags))
    }

    pub fn get_maxreaders(&self) -> MdbResult<c_uint> {
        let mut max_readers: c_uint = 0;
        lift_mdb!(unsafe {
            ffi::mdb_env_get_maxreaders(self.env.0, &mut max_readers)
        }, max_readers)
    }

    pub fn get_maxkeysize(&self) -> c_int {
        unsafe {ffi::mdb_env_get_maxkeysize(self.env.0)}
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: ffi::mdb_filehandle_t) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_copyfd(self.env.0, fd) })
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MdbResult<ffi::mdb_filehandle_t> {
        let mut fd = 0;
        lift_mdb!({ unsafe { ffi::mdb_env_get_fd(self.env.0, &mut fd) }}, fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P) -> MdbResult<()> {
        // FIXME: revert back once `convert` is stable
        // let c_path = path.as_os_str().to_cstring().unwrap();
        let path_str = path.as_ref().to_str().ok_or(MdbError::InvalidPath)?;
        let c_path = CString::new(path_str).map_err(|_| MdbError::InvalidPath)?;

        unsafe {
            lift_mdb!(ffi::mdb_env_copy(self.env.0, c_path.as_ref().as_ptr()))
        }
    }

    fn create_transaction(&self, parent: Option<NativeTransaction>, flags: c_uint) -> MdbResult<NativeTransaction> {
        let mut handle: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::null_mut()
        };

        lift_mdb!(unsafe { ffi::mdb_txn_begin(self.env.0, parent_handle, flags, &mut handle) },
                 NativeTransaction::new_with_handle(handle, flags as usize, self))
    }

    /// Creates a new read-write transaction
    ///
    /// Use `get_reader` to get much faster lock-free alternative
    pub fn new_transaction(&self) -> MdbResult<Transaction> {
        if self.is_readonly {
            return Err(MdbError::StateError("Error: creating read-write transaction in read-only environment".to_owned()))
        }
        self.create_transaction(None, 0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    /// Creates a readonly transaction
    pub fn get_reader(&self) -> MdbResult<ReadonlyTransaction> {
        self.create_transaction(None, ffi::MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    fn _open_db(&self, db_name: & str, flags: DbFlags, force_creation: bool) -> MdbResult<ffi::MDB_dbi> {
        debug!("Opening {} (create={}, read_only={})", db_name, force_creation, self.is_readonly);
        // From LMDB docs for mdb_dbi_open:
        //
        // This function must not be called from multiple concurrent
        // transactions. A transaction that uses this function must finish
        // (either commit or abort) before any other transaction may use
        // this function
        match self.db_cache.lock() {
            Err(_) => Err(MdbError::CacheError),
            Ok(guard) => {
                let cell = &*guard;
                let cache = cell.get();

                unsafe {
                    if let Some(db) = (*cache).get(db_name) {
                        debug!("Cached value for {}: {}", db_name, *db);
                        return Ok(*db);
                    }
                }

                let mut txn = {
                    let txflags = if self.is_readonly { ffi::MDB_RDONLY } else { 0 };
                    self.create_transaction(None, txflags)?
                };
                let opt_name = if !db_name.is_empty() {Some(db_name)} else {None};
                let flags = if force_creation {flags | DB_CREATE} else {flags - DB_CREATE};

                let mut db: ffi::MDB_dbi = 0;
                let db_res = match opt_name {
                    None => unsafe { ffi::mdb_dbi_open(txn.handle, ptr::null(), flags.bits(), &mut db) },
                    Some(db_name) => {
                        let db_name = CString::new(db_name.as_bytes()).unwrap();
                        unsafe {
                            ffi::mdb_dbi_open(txn.handle, db_name.as_ptr(), flags.bits(), &mut db)
                        }
                    }
                };

                try_mdb!(db_res);
                txn.commit()?;

                debug!("Caching: {} -> {}", db_name, db);
                unsafe {
                    (*cache).insert(db_name.to_owned(), db);
                };

                Ok(db)
            }
        }
    }

    /// Opens existing DB
    pub fn get_db(& self, db_name: &str, flags: DbFlags) -> MdbResult<Database> {
        let db = self._open_db(db_name, flags, false)?;
        Ok(Database::new_with_handle(db))
    }

    /// Opens or creates a DB
    pub fn create_db(&self, db_name: &str, flags: DbFlags) -> MdbResult<Database> {
        let db = self._open_db(db_name, flags, true)?;
        Ok(Database::new_with_handle(db))
    }

    /// Opens default DB with specified flags
    pub fn get_default_db(&self, flags: DbFlags) -> MdbResult<Database> {
        self.get_db("", flags)
    }

    pub fn drop_db_from_cache(&self, handle: ffi::MDB_dbi) {
        match self.db_cache.lock() {
            Err(_) => (),
            Ok(guard) => {
                let cell = &*guard;

                unsafe {
                    let cache = cell.get();

                    let mut key = None;
                    for (k, v) in (*cache).iter() {
                        if *v == handle {
                            key = Some(k);
                            break;
                        }
                    }

                    if let Some(key) = key {
                        (*cache).remove(key);
                    }
                }
            }
        }
    }
}

unsafe impl Sync for Environment {}
unsafe impl Send for Environment {}

impl Clone for Environment {
    fn clone(&self) -> Environment {
        Environment {
            env: self.env.clone(),
            db_cache: self.db_cache.clone(),
            is_readonly: self.is_readonly,
        }
    }
}