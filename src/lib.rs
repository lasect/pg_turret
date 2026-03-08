use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::sync::LazyLock;
use std::time::Duration;
use std::ffi::{CStr, CString};

pub mod config;
pub mod log_capture;

use config::http::HttpAdapter;
use config::Adapter;

static HTTP_ADAPTER: LazyLock<HttpAdapter> = LazyLock::new(HttpAdapter::new);

pub static HTTP_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static HTTP_ENDPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static HTTP_API_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static HTTP_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
pub static HTTP_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(100);

pub static POLL_INTERVAL_S: GucSetting<i32> = GucSetting::<i32>::new(10);

fn register_guc() {
    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.enabled\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable HTTP log export\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Set to true to enable exporting logs via HTTP\0") },
        &HTTP_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.endpoint\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"HTTP endpoint URL\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"URL to send logs to via HTTP\0") },
        &HTTP_ENDPOINT,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.api_key\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"HTTP API key\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"API key for authentication with HTTP endpoint\0") },
        &HTTP_API_KEY,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.timeout_ms\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"HTTP request timeout in milliseconds\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Timeout for HTTP requests\0") },
        &HTTP_TIMEOUT_MS,
        100,
        60000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.batch_size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"HTTP batch size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Number of logs to batch before sending\0") },
        &HTTP_BATCH_SIZE,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.poll_interval_s\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Log poll interval in seconds\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"How often the background worker checks for new logs to send\0") },
        &POLL_INTERVAL_S,
        1,
        3600,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

::pgrx::pg_module_magic!();

#[pg_extern]
fn get_captured_logs_count() -> i64 {
    log_capture::get_logs().len() as i64
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    register_guc();
    log_capture::set_hook();

    BackgroundWorkerBuilder::new("pg_turret")
        .set_function("background_worker_main")
        .set_library("pg_turret")
        .enable_spi_access()
        .load();
}

fn sync_worker_config() {
    config::http::HttpAdapter::set_enabled(HTTP_ENABLED.get());
    config::http::set_http_config(config::http::HttpConfig {
        endpoint: HTTP_ENDPOINT
            .get()
            .and_then(|cs| cs.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_default(),
        api_key: HTTP_API_KEY
            .get()
            .and_then(|cs| cs.to_str().ok().map(|s| s.to_string())),
        timeout_ms: HTTP_TIMEOUT_MS.get() as u64,
        batch_size: HTTP_BATCH_SIZE.get() as usize,
    });
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!("pg_turret background worker starting");

    // Initial sync
    sync_worker_config();

    loop {
        let poll_interval = Duration::from_secs(POLL_INTERVAL_S.get() as u64);

        if !BackgroundWorker::wait_latch(Some(poll_interval)) {
            // Timeout reached, or signal received
        }

        if BackgroundWorker::sighup_received() {
            log!("pg_turret configuration reload signaled");
            sync_worker_config();
        }

        BackgroundWorker::transaction(|| {
            let logs = log_capture::consume_logs();
            if !logs.is_empty() {
                if HTTP_ADAPTER.is_enabled() {
                    if let Err(e) = HTTP_ADAPTER.send(&logs) {
                        eprintln!("pg_turret: HTTP adapter error: {}", e);
                    }
                }
            }
        });
    }
}
