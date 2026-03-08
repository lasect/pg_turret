use pgrx::pg_sys::{self, ErrorData};
use std::sync::Mutex;
use chrono::Utc;

static CAPTURED_LOGS: Mutex<Vec<CapturedLog>> = Mutex::new(Vec::new());

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

impl From<&ErrorData> for CapturedLog {
    fn from(ed: &ErrorData) -> Self {
        fn c_str_to_string(ptr: *const std::ffi::c_char) -> Option<String> {
            if ptr.is_null() {
                None
            } else {
                unsafe {
                    std::ffi::CStr::from_ptr(ptr)
                        .to_str()
                        .ok()
                        .map(|s| s.to_string())
                }
            }
        }

        fn c_str_to_string_mut(ptr: *mut std::ffi::c_char) -> Option<String> {
            if ptr.is_null() {
                None
            } else {
                unsafe {
                    std::ffi::CStr::from_ptr(ptr)
                        .to_str()
                        .ok()
                        .map(|s| s.to_string())
                }
            }
        }

        fn c_str_to_string_palloc(ptr: *mut std::ffi::c_char) -> Option<String> {
            if ptr.is_null() {
                None
            } else {
                let s = unsafe {
                    std::ffi::CStr::from_ptr(ptr)
                        .to_str()
                        .ok()
                        .map(|s| s.to_string())
                };
                unsafe { pg_sys::pfree(ptr as *mut _) };
                s
            }
        }

        let database = unsafe {
            if pg_sys::MyDatabaseId == 0.into() {
                None
            } else {
                c_str_to_string_palloc(pg_sys::get_database_name(pg_sys::MyDatabaseId))
            }
        };
        let user = unsafe {
            let user_id = pg_sys::GetUserId();
            if user_id == 0.into() {
                None
            } else {
                c_str_to_string_palloc(pg_sys::GetUserNameFromId(user_id, false))
            }
        };
        let query = unsafe { c_str_to_string(pg_sys::debug_query_string) };

        CapturedLog {
            timestamp: Utc::now().to_rfc3339(),
            elevel: ed.elevel,
            message: c_str_to_string_mut(ed.message).unwrap_or_default(),
            detail: c_str_to_string_mut(ed.detail),
            hint: c_str_to_string_mut(ed.hint),
            context: c_str_to_string_mut(ed.context),
            sqlerrcode: ed.sqlerrcode,
            filename: c_str_to_string(ed.filename),
            lineno: ed.lineno,
            funcname: c_str_to_string(ed.funcname),
            database,
            user,
            query,
        }
    }
}

pub fn add_log(log: CapturedLog) {
    if let Ok(mut logs) = CAPTURED_LOGS.lock() {
        logs.push(log);
        if logs.len() > 1000 {
            logs.drain(0..500);
        }
    }
}

pub fn get_logs() -> Vec<CapturedLog> {
    CAPTURED_LOGS
        .lock()
        .map(|logs| {
            let result = logs.clone();
            // Clear logs after retrieving them to avoid re-sending
            // logs.clear(); 
            // Wait, should we clear them here or in the background worker?
            // If we have multiple adapters, we shouldn't clear here.
            result
        })
        .unwrap_or_default()
}

pub fn consume_logs() -> Vec<CapturedLog> {
    if let Ok(mut logs) = CAPTURED_LOGS.lock() {
        let result = logs.clone();
        logs.clear();
        result
    } else {
        Vec::new()
    }
}

#[allow(dead_code)]
pub fn clear() {
    if let Ok(mut logs) = CAPTURED_LOGS.lock() {
        logs.clear();
    }
}

unsafe extern "C-unwind" fn emit_log_hook_func(edata: *mut pgrx::pg_sys::ErrorData) {
    if !edata.is_null() {
        let log = CapturedLog::from(&*edata);
        add_log(log);
    }
}

pub fn set_hook() {
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(emit_log_hook_func);
    }
}
