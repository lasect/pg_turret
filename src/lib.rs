use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_shmem_init;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::ffi::{CStr, CString};
use std::sync::LazyLock;
use std::time::Duration;

pub mod config;
pub mod log_capture;
pub mod metrics;

use config::http::HttpAdapter;
use config::kafka::KafkaAdapter;
use config::Adapter;
use log_capture::LOG_RING_BUFFER;

static HTTP_ADAPTER: LazyLock<HttpAdapter> = LazyLock::new(HttpAdapter::new);
static KAFKA_ADAPTER: LazyLock<KafkaAdapter> = LazyLock::new(KafkaAdapter::new);

pub static HTTP_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static HTTP_ENDPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static HTTP_API_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static HTTP_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
pub static HTTP_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static HTTP_COMPRESSION: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static KAFKA_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static KAFKA_BROKERS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static KAFKA_TOPIC: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static KAFKA_API_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static KAFKA_API_SECRET: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static KAFKA_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
pub static KAFKA_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(100);



pub static POLL_INTERVAL_S: GucSetting<i32> = GucSetting::<i32>::new(10);
pub static RING_BUFFER_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1024);
pub static NUM_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);
pub static RETRY_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static RETRY_MAX_ATTEMPTS: GucSetting<i32> = GucSetting::<i32>::new(3);
pub static RETRY_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(512);

pub static LOG_LEVEL_MIN: GucSetting<i32> = GucSetting::<i32>::new(10);
pub static LOG_PATTERN: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static LOG_PATTERN_EXCLUDE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

fn register_guc() {
    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.enabled\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable HTTP log export\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(b"Set to true to enable exporting logs via HTTP\0")
        },
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
         unsafe {
             CStr::from_bytes_with_nul_unchecked(b"API key for authentication with HTTP endpoint\0")
         },
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

     GucRegistry::define_bool_guc(
         unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.http.compression\0") },
         unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable gzip compression\0") },
         unsafe {
             CStr::from_bytes_with_nul_unchecked(b"Compress HTTP request bodies with gzip\0")
         },
         &HTTP_COMPRESSION,
         GucContext::Sighup,
         GucFlags::default(),
     );


    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.poll_interval_s\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Log poll interval in seconds\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"How often the background worker checks for new logs to send\0",
            )
        },
        &POLL_INTERVAL_S,
        1,
        3600,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.ring_buffer_size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Ring buffer capacity\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Maximum number of log entries in the shared memory ring buffer\0",
            )
        },
        &RING_BUFFER_SIZE,
        128,
        65536,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.num_workers\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Number of background workers\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Number of background workers for exporting logs\0",
            )
        },
        &NUM_WORKERS,
        1,
        8,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.retry.enabled\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable retry queue\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Enable retry queue for failed log submissions\0",
            )
        },
        &RETRY_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.retry.max_attempts\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Maximum retry attempts\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Maximum number of retry attempts for failed submissions\0",
            )
        },
        &RETRY_MAX_ATTEMPTS,
        1,
        10,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.retry.queue_size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Retry queue size\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Maximum number of failed entries to keep for retry\0",
            )
        },
        &RETRY_QUEUE_SIZE,
        64,
        4096,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.filter.level_min\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Minimum log level to capture\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Minimum log level (10=DEBUG, 17=INFO, 19=WARNING, 20=ERROR, 21=FATAL)\0",
            )
        },
        &LOG_LEVEL_MIN,
        10,
        21,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.filter.pattern\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Log message pattern to include\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Regex pattern - only logs matching this pattern will be captured\0",
            )
        },
        &LOG_PATTERN,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.filter.pattern_exclude\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Log message pattern to exclude\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Regex pattern - logs matching this pattern will be excluded\0",
            )
        },
        &LOG_PATTERN_EXCLUDE,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.enabled\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable Kafka log export\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(b"Set to true to enable exporting logs via Kafka\0")
        },
        &KAFKA_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.brokers\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka broker list\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Comma-separated list of Kafka brokers\0") },
        &KAFKA_BROKERS,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.topic\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka topic\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka topic to publish logs to\0") },
        &KAFKA_TOPIC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.api_key\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka API key\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(b"API key for authentication with Kafka\0")
        },
        &KAFKA_API_KEY,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.api_secret\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka API secret\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(b"API secret for authentication with Kafka\0")
        },
        &KAFKA_API_SECRET,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.timeout_ms\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka request timeout in milliseconds\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Timeout for Kafka requests\0") },
        &KAFKA_TIMEOUT_MS,
        100,
        60000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.kafka.batch_size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Kafka batch size\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Number of logs to batch before sending\0") },
        &KAFKA_BATCH_SIZE,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

::pgrx::pg_module_magic!();

#[pg_extern]
fn get_captured_logs_count() -> i64 {
    log_capture::get_pending_count() as i64
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    register_guc();

    // init shared memory ring buffer for log entries
    // This must be in _PG_init, not deferred
    pg_shmem_init!(LOG_RING_BUFFER);

    // chain our own shmem_startup_hook once pg_shmem_init! is setup so it firing means the shared memory and LWLocks are ready to use.
    // our hook will call the previous one first, then marks shmem as ready.
    unsafe {
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(turret_shmem_startup);
    }

    // Note: We don't set the emit_log_hook here because it would be called
    // during initdb (bootstrap mode) when shared memory isn't fully ready.
    // The hook is set in turret_shmem_startup() after shmem is initialized.
    
    // Initialize metrics
    metrics::init_metrics();

    // Spawn configured number of background workers
    let num_workers = NUM_WORKERS.get().max(1).min(8);
    for worker_id in 0..num_workers {
        let worker_name = if worker_id == 0 {
            "pg_turret".to_string()
        } else {
            format!("pg_turret_{}", worker_id)
        };

        BackgroundWorkerBuilder::new(&worker_name)
            .set_function("background_worker_main")
            .set_library("pg_turret")
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_shmem_access(None)
            .load();
    }
}

static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

unsafe extern "C-unwind" fn turret_shmem_startup() {
    // call the previous hook (which is pg_shmem_init!'s hook that
    // actually initializes the LWLock and shared memory segment).
    if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
        prev();
    }
    // now the ring buffer LWLock is initialized, safe to use.
    log_capture::mark_shmem_ready();
    // Now safe to set the log hook - this runs after postmaster starts
    log_capture::set_hook();
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
            compression: HTTP_COMPRESSION.get(),
        });

        config::kafka::KafkaAdapter::set_enabled(KAFKA_ENABLED.get());
        config::kafka::set_kafka_config(config::kafka::KafkaConfig {
            brokers: KAFKA_BROKERS
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string()))
                .unwrap_or_default(),
            topic: KAFKA_TOPIC
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string()))
                .unwrap_or_default(),
            api_key: KAFKA_API_KEY
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string())),
            api_secret: KAFKA_API_SECRET
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string())),
            timeout_ms: KAFKA_TIMEOUT_MS.get() as u64,
            batch_size: KAFKA_BATCH_SIZE.get() as usize,
        });

        log_capture::set_filter_config(log_capture::FilterConfig {
            level_min: LOG_LEVEL_MIN.get(),
            pattern: LOG_PATTERN
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string())),
            pattern_exclude: LOG_PATTERN_EXCLUDE
                .get()
                .and_then(|cs| cs.to_str().ok().map(|s| s.to_string())),
        });

        log_capture::set_retry_config(log_capture::RetryConfig {
            enabled: RETRY_ENABLED.get(),
            max_attempts: RETRY_MAX_ATTEMPTS.get() as u32,
            queue_size: RETRY_QUEUE_SIZE.get() as usize,
        });

        // Update ring buffer capacity
        log_capture::set_buffer_capacity(RING_BUFFER_SIZE.get() as usize);
    }

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Initialize buffer capacity on first start
    log_capture::set_buffer_capacity(RING_BUFFER_SIZE.get() as usize);
    sync_worker_config();

    loop {
        let poll_interval = Duration::from_secs(POLL_INTERVAL_S.get() as u64);

        if !BackgroundWorker::wait_latch(Some(poll_interval)) {
            break;
        }

        if BackgroundWorker::sighup_received() {
            sync_worker_config();
        }

        // Get list of enabled adapters
        let adapters: Vec<&dyn Adapter> = vec![
            &*HTTP_ADAPTER as &dyn Adapter,
            &*KAFKA_ADAPTER as &dyn Adapter,
        ]
        .into_iter()
        .filter(|a: &&dyn Adapter| a.is_enabled())
        .collect();

        // First, try to send any logs pending in retry queue
        let retry_data = log_capture::get_retry_logs();
        if !retry_data.is_empty() {
            let mut logs = Vec::with_capacity(retry_data.len());
            let mut attempts = Vec::with_capacity(retry_data.len());
            for (log, attempt) in retry_data {
                logs.push(log);
                attempts.push(attempt);
            }

            let mut all_succeeded = true;
            for adapter in &adapters {
                if let Err(e) = adapter.send(&logs) {
                    pgrx::log!("pg_turret: failed to send retry logs to {}: {}", adapter.name(), e.message);
                    all_succeeded = false;
                }
            }

            if !all_succeeded {
                // Re-add with incremented attempts
                for (log, attempt) in logs.into_iter().zip(attempts) {
                    log_capture::add_batch_to_retry_queue(vec![log], attempt + 1);
                }
            } else {
                log_capture::LOGS_SENT.fetch_add(logs.len() as u64, std::sync::atomic::Ordering::SeqCst);
            }
        }

        // Then consume new logs from ring buffer
        let logs = log_capture::consume_logs();
        if !logs.is_empty() {
            let mut all_succeeded = true;
            for adapter in &adapters {
                if let Err(e) = adapter.send(&logs) {
                    pgrx::log!("pg_turret: failed to send logs to {}: {}", adapter.name(), e.message);
                    all_succeeded = false;
                }
            }

            if !all_succeeded {
                log_capture::add_batch_to_retry_queue(logs, 1);
            } else {
                log_capture::LOGS_SENT.fetch_add(logs.len() as u64, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}
