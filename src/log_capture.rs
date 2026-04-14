use chrono::Utc;
use once_cell::sync::Lazy;
use pgrx::pg_sys::{self, ErrorData};
use pgrx::{PGRXSharedMemory, PgLwLock};
use regex::Regex;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::config::StreamType;

/// Flag indicating shared memory has been initialized and the ring buffer is safe to access.
static SHMEM_READY: AtomicBool = AtomicBool::new(false);

/// Flag to temporarily suppress the log hook (prevents recursive logging during Sentry sends).
static HOOK_SUPPRESSED: AtomicBool = AtomicBool::new(false);

pub fn suppress_hook(suppress: bool) {
    HOOK_SUPPRESSED.store(suppress, Ordering::SeqCst);
}

/// Maximum number of log entries in the shared memory ring buffer.
/// This is the compile-time maximum. The actual used size is controlled by GUC.
const RING_BUFFER_MAX_CAPACITY: usize = 1024;
const RING_BUFFER_DEFAULT_CAPACITY: usize = 256;

/// Maximum length for string fields stored in shared memory.
const MAX_SHORT_STR: usize = 256;
const MAX_MSG_STR: usize = 1024;

#[derive(Clone)]
pub struct FilterConfig {
    pub level_min: i32,
    pub pattern: Option<String>,
    pub pattern_exclude: Option<String>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            level_min: 10,
            pattern: None,
            pattern_exclude: None,
        }
    }
}

static FILTER_CONFIG: Lazy<Mutex<FilterConfig>> = Lazy::new(|| Mutex::new(FilterConfig::default()));
static FILTER_PATTERN: Lazy<Mutex<Option<Regex>>> = Lazy::new(|| Mutex::new(None));
static FILTER_PATTERN_EXCLUDE: Lazy<Mutex<Option<Regex>>> = Lazy::new(|| Mutex::new(None));

#[derive(Clone)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    pub queue_size: usize,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            queue_size: 512,
        }
    }
}

static RETRY_CONFIG: Lazy<Mutex<RetryConfig>> = Lazy::new(|| Mutex::new(RetryConfig::default()));
static RETRY_QUEUE: Lazy<Mutex<VecDeque<(CapturedLog, u32)>>> = Lazy::new(|| Mutex::new(VecDeque::new()));
static RETRY_QUEUE_BY_ADAPTER: Lazy<Mutex<BTreeMap<StreamType, VecDeque<(CapturedLog, u32)>>>> =
    Lazy::new(|| Mutex::new(BTreeMap::new()));

pub static LOGS_CAPTURED: AtomicU64 = AtomicU64::new(0);
pub static LOGS_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static LOGS_SENT: AtomicU64 = AtomicU64::new(0);
pub static LOGS_RETRY_FAILED: AtomicU64 = AtomicU64::new(0);

pub fn set_filter_config(config: FilterConfig) {
    let mut filter = match FILTER_CONFIG.lock() {
        Ok(f) => f,
        Err(_) => return,
    };
    *filter = config.clone();
    drop(filter);
    
    // Compile include pattern with error logging
    let mut pattern_guard = match FILTER_PATTERN.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    if let Some(ref p) = config.pattern {
        match Regex::new(p) {
            Ok(re) => *pattern_guard = Some(re),
            Err(e) => {
                pgrx::warning!("pg_turret: invalid filter pattern '{}': {}", p, e);
                *pattern_guard = None;
            }
        }
    } else {
        *pattern_guard = None;
    }
    drop(pattern_guard);
    
    // Compile exclude pattern with error logging
    let mut exclude_guard = match FILTER_PATTERN_EXCLUDE.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    if let Some(ref p) = config.pattern_exclude {
        match Regex::new(p) {
            Ok(re) => *exclude_guard = Some(re),
            Err(e) => {
                pgrx::warning!("pg_turret: invalid filter pattern_exclude '{}': {}", p, e);
                *exclude_guard = None;
            }
        }
    } else {
        *exclude_guard = None;
    }
}

pub fn set_retry_config(config: RetryConfig) {
    let mut retry = match RETRY_CONFIG.lock() {
        Ok(r) => r,
        Err(_) => return,
    };
    *retry = config;
}

pub fn should_capture_log(level: i32, message: &str) -> bool {
    let (config, pattern, pattern_exclude) = {
        let cfg = match FILTER_CONFIG.lock() {
            Ok(c) => c,
            Err(_) => return true,  // Fail open if poisoned
        };
        let pat = match FILTER_PATTERN.lock() {
            Ok(p) => p,
            Err(_) => return true,
        };
        let pat_ex = match FILTER_PATTERN_EXCLUDE.lock() {
            Ok(p) => p,
            Err(_) => return true,
        };
        (cfg.clone(), pat.clone(), pat_ex.clone())
    };

    if level < config.level_min {
        return false;
    }

    if let Some(ref re) = pattern {
        if !re.is_match(message) {
            return false;
        }
    }

    if let Some(ref re) = pattern_exclude {
        if re.is_match(message) {
            return false;
        }
    }

    true
}

pub fn add_to_retry_queue(log: CapturedLog) {
    add_batch_to_retry_queue(vec![log], 0);
}

pub fn add_batch_to_retry_queue(logs: Vec<CapturedLog>, attempts: u32) {
    if logs.is_empty() {
        return;
    }
    
    let config = RETRY_CONFIG.lock().unwrap();
    if !config.enabled {
        return;
    }
    
    let max_attempts = config.max_attempts;
    if attempts >= max_attempts {
        LOGS_RETRY_FAILED.fetch_add(logs.len() as u64, Ordering::SeqCst);
        return;
    }
    
    let mut queue = RETRY_QUEUE.lock().unwrap();
    let queue_size = config.queue_size;
    
    for log in logs {
        if queue.len() >= queue_size {
            LOGS_RETRY_FAILED.fetch_add(1, Ordering::SeqCst);
            queue.pop_front();
        }
        queue.push_back((log, attempts));
    }
}

pub fn get_retry_logs() -> Vec<(CapturedLog, u32)> {
    let config = match RETRY_CONFIG.lock() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    if !config.enabled {
        return vec![];
    }
    
    let mut queue = match RETRY_QUEUE.lock() {
        Ok(q) => q,
        Err(_) => return vec![],
    };
    queue.drain(..).collect()
}

pub fn add_batch_to_retry_queue_by_adapter(stream: StreamType, logs: Vec<CapturedLog>, attempts: u32) {
    if logs.is_empty() {
        return;
    }
    
    let config = match RETRY_CONFIG.lock() {
        Ok(c) => c,
        Err(_) => return,
    };
    if !config.enabled {
        return;
    }
    
    let max_attempts = config.max_attempts;
    if attempts >= max_attempts {
        LOGS_RETRY_FAILED.fetch_add(logs.len() as u64, Ordering::SeqCst);
        return;
    }
    
    let mut queue_size = match RETRY_QUEUE_BY_ADAPTER.lock() {
        Ok(q) => q,
        Err(_) => return,
    };
    
    let queue = queue_size.entry(stream).or_insert_with(VecDeque::new);
    for log in logs {
        if queue.len() >= config.queue_size {
            LOGS_RETRY_FAILED.fetch_add(1, Ordering::SeqCst);
            queue.pop_front();
        }
        queue.push_back((log, attempts));
    }
}

pub fn get_retry_logs_by_adapter(stream: StreamType) -> Vec<(CapturedLog, u32)> {
    let config = match RETRY_CONFIG.lock() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    if !config.enabled {
        return vec![];
    }
    
    let mut queue = match RETRY_QUEUE_BY_ADAPTER.lock() {
        Ok(q) => q,
        Err(_) => return vec![],
    };
    
    queue.remove(&stream).map(|q| q.into_iter().collect()).unwrap_or_default()
}

pub fn clear_retry_queue() {
    let mut queue = RETRY_QUEUE.lock().unwrap();
    queue.clear();
}

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
/// Uses maximum capacity at compile time, but actual usage is controlled by configured size.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct ShmLogRingBuffer {
    /// Write position (next slot to write to).
    write_pos: usize,
    /// Read position (next slot to read from).
    read_pos: usize,
    /// Number of entries currently in the buffer.
    count: usize,
    /// Number of entries dropped due to buffer overflow.
    dropped: u64,
    /// Configured capacity (set via GUC at runtime).
    configured_capacity: usize,
    /// The ring buffer entries (max capacity).
    entries: [ShmLogEntry; RING_BUFFER_MAX_CAPACITY],
}

unsafe impl PGRXSharedMemory for ShmLogRingBuffer {}

impl Default for ShmLogRingBuffer {
    fn default() -> Self {
        Self {
            write_pos: 0,
            read_pos: 0,
            count: 0,
            dropped: 0,
            configured_capacity: RING_BUFFER_DEFAULT_CAPACITY,
            entries: [ShmLogEntry::empty(); RING_BUFFER_MAX_CAPACITY],
        }
    }
}

impl ShmLogRingBuffer {
    /// Set the configured capacity (must be called after shmem init).
    pub fn set_capacity(&mut self, capacity: usize) {
        let capacity = capacity.min(RING_BUFFER_MAX_CAPACITY).max(128);
        self.configured_capacity = capacity;
        // If current count exceeds new capacity, adjust
        if self.count > capacity {
            self.read_pos = (self.write_pos + capacity).wrapping_sub(self.count) % RING_BUFFER_MAX_CAPACITY;
            self.count = capacity;
        }
    }

    pub fn get_capacity(&self) -> usize {
        self.configured_capacity
    }

    /// Push a log entry into the ring buffer. If full, overwrites the oldest entry.
    pub fn push(&mut self, entry: ShmLogEntry) {
        let capacity = self.configured_capacity;
        self.entries[self.write_pos] = entry;
        self.entries[self.write_pos].occupied = true;
        self.write_pos = (self.write_pos + 1) % RING_BUFFER_MAX_CAPACITY;

        if self.count < capacity {
            self.count += 1;
        } else {
            // Buffer full - advance read_pos (dropping oldest)
            self.read_pos = (self.read_pos + 1) % RING_BUFFER_MAX_CAPACITY;
            self.dropped += 1;
        }
    }

    /// Get the number of entries dropped due to buffer overflow.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Reset the dropped counter.
    pub fn reset_dropped(&mut self) {
        self.dropped = 0;
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
            capacity: RING_BUFFER_MAX_CAPACITY,
        }
    }
}

/// Iterator over drained entries. Borrows the entries array.
pub struct ShmDrainIter<'a> {
    entries: &'a [ShmLogEntry; RING_BUFFER_MAX_CAPACITY],
    pos: usize,
    remaining: usize,
    capacity: usize,
}

impl<'a> Iterator for ShmDrainIter<'a> {
    type Item = ShmLogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = self.entries[self.pos];
        self.pos = (self.pos + 1) % self.capacity;
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
#[derive(Debug, Clone, Serialize)]
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

impl CapturedLog {
    pub fn level_str(&self) -> &'static str {
        match self.elevel {
            10..=14 => "DEBUG",
            15 | 16 => "LOG",
            17 => "INFO",
            18 => "NOTICE",
            19 => "WARNING",
            20 => "ERROR",
            21 => "FATAL",
            22 => "PANIC",
            _ => "UNKNOWN",
        }
    }
}

#[derive(Serialize)]
pub struct SerializableLog<'a> {
    timestamp: &'a str,
    level: &'a str,
    message: &'a str,
    detail: &'a Option<String>,
    hint: &'a Option<String>,
    context: &'a Option<String>,
    sqlerrcode: i32,
    filename: &'a Option<String>,
    lineno: i32,
    funcname: &'a Option<String>,
    database: &'a Option<String>,
    user: &'a Option<String>,
    query: &'a Option<String>,
}

impl<'a> From<&'a CapturedLog> for SerializableLog<'a> {
    fn from(log: &'a CapturedLog) -> Self {
        Self {
            timestamp: &log.timestamp,
            level: log.level_str(),
            message: &log.message,
            detail: &log.detail,
            hint: &log.hint,
            context: &log.context,
            sqlerrcode: log.sqlerrcode,
            filename: &log.filename,
            lineno: log.lineno,
            funcname: &log.funcname,
            database: &log.database,
            user: &log.user,
            query: &log.query,
        }
    }
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
    let drained: Vec<CapturedLog> = buf.drain().map(|e| CapturedLog::from(&e)).collect();
    LOGS_CAPTURED.fetch_add(drained.len() as u64, Ordering::SeqCst);
    drained
}

/// Set the ring buffer capacity (called after GUC is read).
pub fn set_buffer_capacity(capacity: usize) {
    let mut buf = LOG_RING_BUFFER.exclusive();
    buf.set_capacity(capacity);
}

/// Get count of pending logs (for the SQL function).
pub fn get_pending_count() -> usize {
    LOG_RING_BUFFER.share().count
}

/// Get ring buffer capacity.
pub fn get_buffer_capacity() -> usize {
    LOG_RING_BUFFER.share().get_capacity()
}

/// Get total logs captured (cumulative).
pub fn get_logs_captured() -> u64 {
    LOGS_CAPTURED.load(Ordering::SeqCst)
}

/// Get total logs dropped (cumulative).
pub fn get_logs_dropped() -> u64 {
    LOGS_DROPPED.load(Ordering::SeqCst)
}

/// Get total logs sent successfully (cumulative).
pub fn get_logs_sent() -> u64 {
    LOGS_SENT.load(Ordering::SeqCst)
}

/// Get total retry failures (cumulative).
pub fn get_logs_retry_failed() -> u64 {
    LOGS_RETRY_FAILED.load(Ordering::SeqCst)
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

    // Note: We avoid catalog lookups (get_database_name, GetUserNameFromId) here
    // because they can crash during initdb/early startup when catalogs aren't ready.
    // The ErrorData struct itself contains some fields but not database/user names.
    // Those can be added later if needed through other means.

    let query_str = {
        let ptr: *const std::ffi::c_char = unsafe { pg_sys::debug_query_string };
        if ptr.is_null() {
            None
        } else {
            // debug_query_string is a global that may be null or point to freed memory
            // We validate by checking if the pointer is non-null and attempt conversion
            unsafe { CStr::from_ptr(ptr).to_str().ok().map(|s| s.to_string()) }
        }
    };
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
        database: ShmOptStr::none(),
        user: ShmOptStr::none(),
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

    if HOOK_SUPPRESSED.load(Ordering::SeqCst) {
        return;
    }

    if !SHMEM_READY.load(Ordering::SeqCst) {
        return;
    }

    let level = (*edata).elevel;
    
    // Fast path: check level filter first (no string allocation)
    let level_min = match FILTER_CONFIG.lock() {
        Ok(cfg) => cfg.level_min,
        Err(_) => return,  // Fail open - allow log through if config poisoned
    };
    if level < level_min {
        return;
    }

    // Check if pattern filtering is configured
    let needs_pattern_check = {
        match FILTER_CONFIG.lock() {
            Ok(cfg) => cfg.pattern.is_some() || cfg.pattern_exclude.is_some(),
            Err(_) => false,  // Fail closed - no pattern check if config unavailable
        }
    };

    if needs_pattern_check {
        // Only extract message if pattern filtering is enabled
        let message = if (*edata).message.is_null() {
            String::new()
        } else {
            CStr::from_ptr((*edata).message)
                .to_str()
                .unwrap_or_default()
                .to_string()
        };

        if !should_capture_log(level, &message) {
            return;
        }
    }

    let entry = build_shm_entry(&*edata);

    let mut buf = LOG_RING_BUFFER.exclusive();
    buf.push(entry);
    
    if buf.count >= buf.get_capacity() {
        LOGS_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn set_hook() {
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(emit_log_hook_func);
    }
}
