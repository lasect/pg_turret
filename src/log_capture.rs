use chrono::Utc;
use pgrx::pg_sys::{self, ErrorData};
use pgrx::{PGRXSharedMemory, PgLwLock};
use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, Ordering};

/// Flag indicating shared memory has been initialized and the ring buffer is safe to access.
static SHMEM_READY: AtomicBool = AtomicBool::new(false);

/// Maximum number of log entries in the shared memory ring buffer.
const RING_BUFFER_CAPACITY: usize = 1024;

/// Maximum length for string fields stored in shared memory.
const MAX_SHORT_STR: usize = 256;
const MAX_MSG_STR: usize = 1024;

/// A fixed-size string stored inline in shared memory.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct ShmStr<const N: usize> {
    len: u16,
    data: [u8; N],
}

unsafe impl<const N: usize> PGRXSharedMemory for ShmStr<N> {}

impl<const N: usize> ShmStr<N> {
    const fn empty() -> Self {
        Self {
            len: 0,
            data: [0u8; N],
        }
    }

    fn from_str(s: &str) -> Self {
        let bytes = s.as_bytes();
        let copy_len = bytes.len().min(N);
        let mut data = [0u8; N];
        data[..copy_len].copy_from_slice(&bytes[..copy_len]);
        Self {
            len: copy_len as u16,
            data,
        }
    }

    pub fn as_str(&self) -> &str {
        let len = (self.len as usize).min(N);
        // Safety: we only store valid UTF-8 from from_str
        std::str::from_utf8(&self.data[..len]).unwrap_or("")
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A fixed-size optional string field.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct ShmOptStr<const N: usize> {
    present: bool,
    inner: ShmStr<N>,
}

unsafe impl<const N: usize> PGRXSharedMemory for ShmOptStr<N> {}

impl<const N: usize> ShmOptStr<N> {
    const fn none() -> Self {
        Self {
            present: false,
            inner: ShmStr::empty(),
        }
    }

    fn from_option(opt: Option<&str>) -> Self {
        match opt {
            Some(s) => Self {
                present: true,
                inner: ShmStr::from_str(s),
            },
            None => Self::none(),
        }
    }

    pub fn as_option(&self) -> Option<&str> {
        if self.present {
            Some(self.inner.as_str())
        } else {
            None
        }
    }
}

/// A single log entry stored in shared memory.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct ShmLogEntry {
    pub occupied: bool,
    pub timestamp: ShmStr<40>,
    pub elevel: i32,
    pub message: ShmStr<MAX_MSG_STR>,
    pub detail: ShmOptStr<MAX_SHORT_STR>,
    pub hint: ShmOptStr<MAX_SHORT_STR>,
    pub context: ShmOptStr<MAX_SHORT_STR>,
    pub sqlerrcode: i32,
    pub filename: ShmOptStr<MAX_SHORT_STR>,
    pub lineno: i32,
    pub funcname: ShmOptStr<MAX_SHORT_STR>,
    pub database: ShmOptStr<MAX_SHORT_STR>,
    pub user: ShmOptStr<MAX_SHORT_STR>,
    pub query: ShmOptStr<MAX_SHORT_STR>,
}

unsafe impl PGRXSharedMemory for ShmLogEntry {}

impl ShmLogEntry {
    const fn empty() -> Self {
        Self {
            occupied: false,
            timestamp: ShmStr::empty(),
            elevel: 0,
            message: ShmStr::empty(),
            detail: ShmOptStr::none(),
            hint: ShmOptStr::none(),
            context: ShmOptStr::none(),
            sqlerrcode: 0,
            filename: ShmOptStr::none(),
            lineno: 0,
            funcname: ShmOptStr::none(),
            database: ShmOptStr::none(),
            user: ShmOptStr::none(),
            query: ShmOptStr::none(),
        }
    }
}

/// Ring buffer for log entries in shared memory.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct ShmLogRingBuffer {
    /// Write position (next slot to write to).
    write_pos: usize,
    /// Read position (next slot to read from).
    read_pos: usize,
    /// Number of entries currently in the buffer.
    count: usize,
    /// The ring buffer entries.
    entries: [ShmLogEntry; RING_BUFFER_CAPACITY],
}

unsafe impl PGRXSharedMemory for ShmLogRingBuffer {}

impl Default for ShmLogRingBuffer {
    fn default() -> Self {
        Self {
            write_pos: 0,
            read_pos: 0,
            count: 0,
            entries: [ShmLogEntry::empty(); RING_BUFFER_CAPACITY],
        }
    }
}

impl ShmLogRingBuffer {
    /// Push a log entry into the ring buffer. If full, overwrites the oldest entry.
    pub fn push(&mut self, entry: ShmLogEntry) {
        self.entries[self.write_pos] = entry;
        self.entries[self.write_pos].occupied = true;
        self.write_pos = (self.write_pos + 1) % RING_BUFFER_CAPACITY;

        if self.count < RING_BUFFER_CAPACITY {
            self.count += 1;
        } else {
            // Buffer full - advance read_pos (dropping oldest)
            self.read_pos = (self.read_pos + 1) % RING_BUFFER_CAPACITY;
        }
    }

    /// Drain all available entries from the ring buffer.
    pub fn drain(&mut self) -> ShmDrainIter<'_> {
        let count = self.count;
        let start = self.read_pos;
        self.read_pos = self.write_pos;
        self.count = 0;
        ShmDrainIter {
            entries: &self.entries,
            pos: start,
            remaining: count,
        }
    }
}

/// Iterator over drained entries. Borrows the entries array.
pub struct ShmDrainIter<'a> {
    entries: &'a [ShmLogEntry; RING_BUFFER_CAPACITY],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ShmDrainIter<'a> {
    type Item = ShmLogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = self.entries[self.pos];
        self.pos = (self.pos + 1) % RING_BUFFER_CAPACITY;
        self.remaining -= 1;
        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// The shared memory ring buffer, protected by a PostgreSQL LWLock.
pub static LOG_RING_BUFFER: PgLwLock<ShmLogRingBuffer> =
    unsafe { PgLwLock::new(c"pg_turret_log_ring") };

/// Decoded log entry for use in Rust (heap-allocated strings).
#[derive(Debug, Clone)]
pub struct CapturedLog {
    pub timestamp: String,
    pub elevel: i32,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: Option<String>,
    pub sqlerrcode: i32,
    pub filename: Option<String>,
    pub lineno: i32,
    pub funcname: Option<String>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub query: Option<String>,
}

impl From<&ShmLogEntry> for CapturedLog {
    fn from(e: &ShmLogEntry) -> Self {
        Self {
            timestamp: e.timestamp.as_str().to_string(),
            elevel: e.elevel,
            message: e.message.as_str().to_string(),
            detail: e.detail.as_option().map(|s| s.to_string()),
            hint: e.hint.as_option().map(|s| s.to_string()),
            context: e.context.as_option().map(|s| s.to_string()),
            sqlerrcode: e.sqlerrcode,
            filename: e.filename.as_option().map(|s| s.to_string()),
            lineno: e.lineno,
            funcname: e.funcname.as_option().map(|s| s.to_string()),
            database: e.database.as_option().map(|s| s.to_string()),
            user: e.user.as_option().map(|s| s.to_string()),
            query: e.query.as_option().map(|s| s.to_string()),
        }
    }
}

/// Consume all pending log entries from the shared memory ring buffer.
/// Called by the background worker.
pub fn consume_logs() -> Vec<CapturedLog> {
    let mut buf = LOG_RING_BUFFER.exclusive();
    buf.drain().map(|e| CapturedLog::from(&e)).collect()
}

/// Get count of pending logs (for the SQL function).
pub fn get_pending_count() -> usize {
    LOG_RING_BUFFER.share().count
}

fn c_str_to_option(ptr: *const std::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        unsafe {
            CStr::from_ptr(ptr)
                .to_str()
                .ok()
                .map(|s| s.to_string())
        }
    }
}

fn c_str_mut_to_option(ptr: *mut std::ffi::c_char) -> Option<String> {
    c_str_to_option(ptr as *const _)
}

/// Build a ShmLogEntry from a PostgreSQL ErrorData.
fn build_shm_entry(ed: &ErrorData) -> ShmLogEntry {
    let timestamp = Utc::now().to_rfc3339();

    // Only attempt catalog lookups when the database system is fully
    // initialized. During early startup (initdb, recovery, bgworker
    // init), MyDatabaseId is InvalidOid and catalog access would crash.
    let database_str = unsafe {
        let db_id = pg_sys::MyDatabaseId;
        if db_id != pg_sys::InvalidOid && pg_sys::IsTransactionState() {
            let name_ptr = pg_sys::get_database_name(db_id);
            if !name_ptr.is_null() {
                let s = CStr::from_ptr(name_ptr)
                    .to_str()
                    .ok()
                    .map(|s| s.to_string());
                pg_sys::pfree(name_ptr as *mut _);
                s
            } else {
                None
            }
        } else {
            None
        }
    };

    let user_str = unsafe {
        let user_id = pg_sys::GetUserId();
        if user_id != pg_sys::InvalidOid && pg_sys::IsTransactionState() {
            let name_ptr = pg_sys::GetUserNameFromId(user_id, true);
            if !name_ptr.is_null() {
                let s = CStr::from_ptr(name_ptr)
                    .to_str()
                    .ok()
                    .map(|s| s.to_string());
                pg_sys::pfree(name_ptr as *mut _);
                s
            } else {
                None
            }
        } else {
            None
        }
    };

    let query_str = unsafe { c_str_to_option(pg_sys::debug_query_string) };
    let message_str = c_str_mut_to_option(ed.message).unwrap_or_default();
    let detail_str = c_str_mut_to_option(ed.detail);
    let hint_str = c_str_mut_to_option(ed.hint);
    let context_str = c_str_mut_to_option(ed.context);
    let filename_str = c_str_to_option(ed.filename);
    let funcname_str = c_str_to_option(ed.funcname);

    ShmLogEntry {
        occupied: true,
        timestamp: ShmStr::from_str(&timestamp),
        elevel: ed.elevel,
        message: ShmStr::from_str(&message_str),
        detail: ShmOptStr::from_option(detail_str.as_deref()),
        hint: ShmOptStr::from_option(hint_str.as_deref()),
        context: ShmOptStr::from_option(context_str.as_deref()),
        sqlerrcode: ed.sqlerrcode,
        filename: ShmOptStr::from_option(filename_str.as_deref()),
        lineno: ed.lineno,
        funcname: ShmOptStr::from_option(funcname_str.as_deref()),
        database: ShmOptStr::from_option(database_str.as_deref()),
        user: ShmOptStr::from_option(user_str.as_deref()),
        query: ShmOptStr::from_option(query_str.as_deref()),
    }
}

/// Mark shared memory as initialized. Called after shmem_startup_hook completes.
pub fn mark_shmem_ready() {
    SHMEM_READY.store(true, Ordering::SeqCst);
}

unsafe extern "C-unwind" fn emit_log_hook_func(edata: *mut pgrx::pg_sys::ErrorData) {
    if edata.is_null() {
        return;
    }

    // Skip if shared memory isn't initialized yet. Between _PG_init (which
    // installs this hook) and shmem_startup_hook (which creates the ring
    // buffer), any log message would access uninitialized shmem and SIGSEGV.
    if !SHMEM_READY.load(Ordering::SeqCst) {
        return;
    }

    let entry = build_shm_entry(&*edata);

    // Write to shared memory ring buffer.
    let mut buf = LOG_RING_BUFFER.exclusive();
    buf.push(entry);
}

pub fn set_hook() {
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(emit_log_hook_func);
    }
}
