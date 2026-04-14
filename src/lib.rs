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
use config::sentry::SentryAdapter;
use config::Adapter;
use log_capture::LOG_RING_BUFFER;

static HTTP_ADAPTER: LazyLock<HttpAdapter> = LazyLock::new(HttpAdapter::new);
static KAFKA_ADAPTER: LazyLock<KafkaAdapter> = LazyLock::new(KafkaAdapter::new);
static SENTRY_ADAPTER: LazyLock<SentryAdapter> = LazyLock::new(SentryAdapter::new);
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

pub static SENTRY_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static SENTRY_DSN: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static SENTRY_ENVIRONMENT: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub static SENTRY_RELEASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static SENTRY_SERVER_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub static SENTRY_SAMPLE_RATE: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static SENTRY_MIN_LEVEL: GucSetting<i32> = GucSetting::<i32>::new(20);
pub static SENTRY_INCLUDE_SQL: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static SENTRY_SEND_DEFAULT_PII: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static SENTRY_MAX_EVENTS_PER_SEC: GucSetting<i32> = GucSetting::<i32>::new(100);



pub static POLL_INTERVAL_S: GucSetting<i32> = GucSetting::<i32>::new(10);
pub static RING_BUFFER_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1024);
pub static NUM_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);
pub static RETRY_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static RETRY_MAX_ATTEMPTS: GucSetting<i32> = GucSetting::<i32>::new(3);
pub static RETRY_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(512);

pub static LOG_LEVEL_MIN: GucSetting<i32> = GucSetting::<i32>::new(10);
pub static LOG_PATTERN: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static LOG_PATTERN_EXCLUDE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

#[cfg(unix)]
fn write_config_atomic(data_dir: &str, filename: &str, content: &str) -> Result<(), String> {
    use std::fs;

    let temp_path = format!("{}/{}.tmp.{}", data_dir, filename, std::process::id());
    let final_path = format!("{}/{}", data_dir, filename);

    // Write to temporary file
    fs::write(&temp_path, content).map_err(|e| format!("Failed to write temp file: {}", e))?;

    // Atomic rename (POSIX guarantees this is atomic)
    fs::rename(&temp_path, &final_path).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        format!("Failed to finalize config: {}", e)
    })
}

#[cfg(not(unix))]
fn write_config_atomic(data_dir: &str, filename: &str, content: &str) -> Result<(), String> {
    let final_path = format!("{}/{}", data_dir, filename);
    std::fs::write(&final_path, content).map_err(|e| format!("Failed to write config: {}", e))
}

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

    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.enabled\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Enable Sentry log export\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Set to true to enable exporting logs to Sentry\0",
            )
        },
        &SENTRY_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.dsn\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry DSN\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry DSN for log export\0") },
        &SENTRY_DSN,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.environment\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry environment\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Sentry environment (e.g. production, staging)\0",
            )
        },
        &SENTRY_ENVIRONMENT,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.release\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry release\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Sentry release identifier (e.g. pg_turret@1.0.0)\0",
            )
        },
        &SENTRY_RELEASE,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.server_name\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry server name\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Sentry server name (e.g. db-prod-1)\0",
            )
        },
        &SENTRY_SERVER_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.sample_rate\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry sample rate\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Client-side sample rate for Sentry events as percentage (0-100)\0",
            )
        },
        &SENTRY_SAMPLE_RATE,
        0,
        100,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.min_level\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Minimum log level for Sentry\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Minimum PostgreSQL log level (10-22) to forward to Sentry\0",
            )
        },
        &SENTRY_MIN_LEVEL,
        10,
        22,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        unsafe { CStr::from_bytes_with_nul_unchecked(b"pg_turret.sentry.include_sql\0") },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Include SQL text in Sentry events\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Include SQL query text in Sentry events when available\0",
            )
        },
        &SENTRY_INCLUDE_SQL,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"pg_turret.sentry.send_default_pii\0",
            )
        },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Sentry send_default_pii\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Whether Sentry should send default PII fields\0",
            )
        },
        &SENTRY_SEND_DEFAULT_PII,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"pg_turret.sentry.max_events_per_sec\0",
            )
        },
        unsafe { CStr::from_bytes_with_nul_unchecked(b"Max Sentry events per second\0") },
        unsafe {
            CStr::from_bytes_with_nul_unchecked(
                b"Maximum number of Sentry events per background worker per second\0",
            )
        },
        &SENTRY_MAX_EVENTS_PER_SEC,
        0,
        10_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

::pgrx::pg_module_magic!();

#[pg_extern]
fn get_captured_logs_count() -> i64 {
    log_capture::get_pending_count() as i64
}

#[pg_extern]
fn get_sentry_status() -> TableIterator<'static, (name!(key, String), name!(value, String))> {
    let cfg = config::sentry::get_sentry_config();
    let enabled = config::sentry::SentryAdapter::is_enabled_global();
    let rows = vec![
        ("enabled".to_string(), enabled.to_string()),
        ("dsn".to_string(), cfg.dsn.clone()),
        ("environment".to_string(), cfg.environment.clone().unwrap_or_default()),
        ("min_level".to_string(), cfg.min_level.to_string()),
        ("max_events_per_sec".to_string(), cfg.max_events_per_sec.to_string()),
        ("sample_rate".to_string(), cfg.sample_rate.to_string()),
        ("include_sql".to_string(), cfg.include_sql.to_string()),
    ];
    TableIterator::new(rows.into_iter().map(|(k, v)| (k, v)))
}

#[pg_extern]
fn configure_sentry(
    dsn: &str,
    enabled: default!(bool, true),
    environment: default!(&str, "''"),
    release: default!(&str, "''"),
    server_name: default!(&str, "''"),
    sample_rate: default!(i32, 100),
    min_level: default!(i32, 20),
    include_sql: default!(bool, false),
    send_default_pii: default!(bool, false),
    max_events_per_sec: default!(i32, 100),
) {
    // Validate DSN before writing config
    let sentry_cfg = config::sentry::SentryConfig {
        dsn: dsn.to_string(),
        environment: if environment.is_empty() { None } else { Some(environment.to_string()) },
        release: if release.is_empty() { None } else { Some(release.to_string()) },
        server_name: if server_name.is_empty() { None } else { Some(server_name.to_string()) },
        sample_rate: (sample_rate.clamp(0, 100) as f32) / 100.0,
        min_level,
        include_sql,
        send_default_pii,
        max_events_per_sec: max_events_per_sec.max(0) as u32,
    };
    if let Err(e) = sentry_cfg.validate_dsn() {
        pgrx::error!("{}", e);
    }

    // Write Sentry config to a separate JSON file that the background worker reads directly
    let data_dir = unsafe {
        let dir = pgrx::pg_sys::DataDir;
        if dir.is_null() {
            pgrx::error!("pg_turret: DataDir is null, cannot write sentry config");
        }
        std::ffi::CStr::from_ptr(dir).to_string_lossy().to_string()
    };

    let config_filename = "pg_turret_sentry.json";
    let config_json = serde_json::to_string(&sentry_cfg).unwrap();

    if let Err(e) = write_config_atomic(&data_dir, config_filename, &config_json) {
        pgrx::error!("pg_turret: Failed to write sentry config file: {}", e);
    }

    // Also set directly for the current process
    config::sentry::SentryAdapter::set_enabled(enabled);
    config::sentry::set_sentry_config(sentry_cfg);

    // Trigger config reload in background worker via pg_reload_conf()
    let _ = Spi::run("SELECT pg_reload_conf();");
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

        // Load Sentry config from JSON file if it exists (written by configure_sentry())
        let data_dir = unsafe {
            let dir = pgrx::pg_sys::DataDir;
            if dir.is_null() {
                None
            } else {
                Some(std::ffi::CStr::from_ptr(dir).to_string_lossy().to_string())
            }
        };

        if let Some(dir) = data_dir {
            let config_path = format!("{}/pg_turret_sentry.json", dir);
            if let Ok(content) = std::fs::read_to_string(&config_path) {
                if let Ok(cfg) = serde_json::from_str::<config::sentry::SentryConfig>(&content) {
                    if !cfg.dsn.is_empty() {
                        config::sentry::SentryAdapter::set_enabled(true);
                        config::sentry::set_sentry_config(cfg);
                    }
                }
            }
        }

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
            &*SENTRY_ADAPTER as &dyn Adapter,
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
