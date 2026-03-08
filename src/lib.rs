use pgrx::bgworkers::*;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::time::Duration;

::pgrx::pg_module_magic!();

mod log_capture {
    use pgrx::pg_sys::ErrorData;
    use std::sync::Mutex;

    static CAPTURED_LOGS: Mutex<Vec<CapturedLog>> = Mutex::new(Vec::new());

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    pub struct CapturedLog {
        pub elevel: i32,
        pub message: String,
        pub detail: Option<String>,
        pub hint: Option<String>,
        pub context: Option<String>,
        pub sqlerrcode: i32,
        pub filename: Option<String>,
        pub lineno: i32,
        pub funcname: Option<String>,
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

            CapturedLog {
                elevel: ed.elevel,
                message: c_str_to_string_mut(ed.message).unwrap_or_default(),
                detail: c_str_to_string_mut(ed.detail),
                hint: c_str_to_string_mut(ed.hint),
                context: c_str_to_string_mut(ed.context),
                sqlerrcode: ed.sqlerrcode,
                filename: c_str_to_string(ed.filename),
                lineno: ed.lineno,
                funcname: c_str_to_string(ed.funcname),
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
            .map(|logs| logs.clone())
            .unwrap_or_default()
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
}

#[pg_extern]
fn get_captured_logs_count() -> i64 {
    log_capture::get_logs().len() as i64
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    log_capture::set_hook();

    BackgroundWorkerBuilder::new("pg_turret")
        .set_function("background_worker_main")
        .set_library("pg_turret")
        .enable_spi_access()
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!("pg_turret background worker starting");

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        if BackgroundWorker::sighup_received() {}

        BackgroundWorker::transaction(|| {
            let logs = log_capture::get_logs();
            if !logs.is_empty() {
                for log in &logs {
                    let level = match log.elevel {
                        0..=14 => "DEBUG",
                        15..=17 => "INFO",
                        18..=24 => "LOG",
                        25..=27 => "WARNING",
                        28 => "ERROR",
                        29 => "FATAL",
                        30 => "PANIC",
                        _ => "UNKNOWN",
                    };
                    eprintln!("[{}] {}", level, log.message);
                }
            }
        });
    }

    log!("pg_turret background worker exiting");
}
