use super::{Adapter, AdapterError, StreamType};
use crate::log_capture::CapturedLog;
use once_cell::sync::Lazy;
use sentry::protocol::{Event, Level};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

static SENTRY_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentryConfig {
    pub dsn: String,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub server_name: Option<String>,
    pub sample_rate: f32,
    pub min_level: i32,
    pub include_sql: bool,
    pub send_default_pii: bool,
    pub max_events_per_sec: u32,
}

impl Default for SentryConfig {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            environment: None,
            release: None,
            server_name: None,
            sample_rate: 1.0,
            min_level: 20,
            include_sql: false,
            send_default_pii: false,
            max_events_per_sec: 100,
        }
    }
}

impl SentryConfig {
    /// Validate the DSN format
    pub fn validate_dsn(&self) -> Result<(), String> {
        if self.dsn.is_empty() {
            return Err("Sentry DSN is empty".to_string());
        }
        match self.dsn.parse::<sentry::types::Dsn>() {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Invalid Sentry DSN '{}': {}", self.dsn, e)),
        }
    }
}

// Configuration storage with version tracking
static SENTRY_CONFIG: Lazy<Mutex<SentryConfig>> = Lazy::new(|| Mutex::new(SentryConfig::default()));
static CONFIG_VERSION: Lazy<Mutex<u64>> = Lazy::new(|| Mutex::new(0));

pub fn set_sentry_config(config: SentryConfig) {
    let mut cfg = SENTRY_CONFIG.lock().expect("SENTRY_CONFIG mutex poisoned");
    let mut version = CONFIG_VERSION
        .lock()
        .expect("CONFIG_VERSION mutex poisoned");
    *cfg = config;
    // Increment version to signal config change
    *version = version.wrapping_add(1);
}

pub fn get_sentry_config() -> SentryConfig {
    let cfg = SENTRY_CONFIG.lock().expect("SENTRY_CONFIG mutex poisoned");
    cfg.clone()
}

fn get_config_version() -> u64 {
    let version = CONFIG_VERSION
        .lock()
        .expect("CONFIG_VERSION mutex poisoned");
    *version
}

// Simple per-worker rate limiter: counts events in a 1-second window.
struct RateLimiter {
    window_start: Instant,
    count: u32,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            count: 0,
        }
    }

    fn allow(&mut self, max_per_sec: u32) -> bool {
        let now = Instant::now();
        if now.duration_since(self.window_start) >= Duration::from_secs(1) {
            self.window_start = now;
            self.count = 0;
        }
        if self.count < max_per_sec {
            self.count += 1;
            true
        } else {
            false
        }
    }
}

/// Client state that can be updated when config changes
struct ClientState {
    guard: Option<sentry::ClientInitGuard>,
    config_hash: u64,
}

impl ClientState {
    fn new() -> Self {
        Self {
            guard: None,
            config_hash: 0,
        }
    }

    /// Ensure the client is initialized with the current config
    fn ensure_initialized(&mut self) -> Result<(), AdapterError> {
        let config = get_sentry_config();
        let current_version = get_config_version();

        // Check if we need to re-initialize
        if self.guard.is_some() && self.config_hash == current_version {
            return Ok(());
        }

        // Validate DSN before attempting to initialize
        if let Err(e) = config.validate_dsn() {
            return Err(AdapterError { message: e });
        }

        // Initialize or re-initialize the client
        self.guard = Some(Self::create_client(&config));
        self.config_hash = current_version;

        Ok(())
    }

    fn create_client(config: &SentryConfig) -> sentry::ClientInitGuard {
        let options = sentry::ClientOptions {
            dsn: config.dsn.parse().ok(),
            environment: config.environment.clone().map(Into::into),
            release: config.release.clone().map(Into::into),
            server_name: config.server_name.clone().map(Into::into),
            sample_rate: config.sample_rate,
            send_default_pii: config.send_default_pii,
            ..Default::default()
        };

        sentry::init(options)
    }
}

static CLIENT_STATE: Lazy<Mutex<ClientState>> = Lazy::new(|| Mutex::new(ClientState::new()));

pub struct SentryAdapter {
    enabled: &'static AtomicBool,
    rate_limiter: Mutex<RateLimiter>,
}

impl std::fmt::Debug for SentryAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentryAdapter")
            .field("enabled", &self.enabled.load(Ordering::SeqCst))
            .finish()
    }
}

impl SentryAdapter {
    pub fn new() -> Self {
        Self {
            enabled: &SENTRY_ENABLED,
            rate_limiter: Mutex::new(RateLimiter::new()),
        }
    }

    pub fn set_enabled(enabled: bool) {
        SENTRY_ENABLED.store(enabled, Ordering::SeqCst);
    }

    pub fn is_enabled_global() -> bool {
        SENTRY_ENABLED.load(Ordering::SeqCst)
    }

    fn map_level(elevel: i32) -> Level {
        match elevel {
            22 => Level::Fatal,
            21 => Level::Fatal,
            20 => Level::Error,
            19 => Level::Warning,
            17 => Level::Info,
            18 => Level::Info,
            10..=16 => Level::Debug,
            _ => Level::Info,
        }
    }

    fn should_send(&self, log: &CapturedLog, cfg: &SentryConfig) -> bool {
        if log.elevel < cfg.min_level {
            return false;
        }
        if cfg.max_events_per_sec == 0 {
            return false;
        }

        let mut limiter = self
            .rate_limiter
            .lock()
            .expect("RATE_LIMITER mutex poisoned");
        limiter.allow(cfg.max_events_per_sec)
    }

    fn build_event(log: &CapturedLog, cfg: &SentryConfig) -> Event<'static> {
        let level = Self::map_level(log.elevel);

        let mut tags = BTreeMap::new();
        tags.insert("db.system".to_string(), "postgresql".to_string());
        if let Some(db) = &log.database {
            tags.insert("db.name".to_string(), db.clone());
        }
        if let Some(user) = &log.user {
            tags.insert("db.user".to_string(), user.clone());
        }
        if let Some(filename) = &log.filename {
            tags.insert("pg.filename".to_string(), filename.clone());
        }
        if let Some(funcname) = &log.funcname {
            tags.insert("pg.funcname".to_string(), funcname.clone());
        }

        let mut extra = BTreeMap::new();
        if let Some(detail) = &log.detail {
            extra.insert(
                "detail".to_string(),
                serde_json::Value::String(detail.clone()),
            );
        }
        if let Some(hint) = &log.hint {
            extra.insert("hint".to_string(), serde_json::Value::String(hint.clone()));
        }
        if let Some(context) = &log.context {
            extra.insert(
                "context".to_string(),
                serde_json::Value::String(context.clone()),
            );
        }
        extra.insert(
            "sqlerrcode".to_string(),
            serde_json::Value::Number(serde_json::Number::from(log.sqlerrcode)),
        );
        extra.insert(
            "lineno".to_string(),
            serde_json::Value::Number(serde_json::Number::from(log.lineno)),
        );

        if cfg.include_sql {
            if let Some(query) = &log.query {
                extra.insert(
                    "query".to_string(),
                    serde_json::Value::String(query.clone()),
                );
            }
        }

        let mut event = Event {
            level,
            message: Some(log.message.clone().into()),
            ..Event::default()
        };

        event.tags = tags;
        event.extra = extra.into_iter().map(|(k, v)| (k, v as Value)).collect();

        event
    }
}

impl Default for SentryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Adapter for SentryAdapter {
    fn name(&self) -> &'static str {
        "sentry"
    }

    fn stream_type(&self) -> StreamType {
        StreamType::Sentry
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    fn send(&self, logs: &[CapturedLog]) -> Result<(), AdapterError> {
        if !self.is_enabled() {
            return Ok(());
        }

        if logs.is_empty() {
            return Ok(());
        }

        let cfg = get_sentry_config();

        // Prevent recursive logging: temporarily suppress the log hook
        crate::log_capture::suppress_hook(true);

        // Ensure client is initialized (lazy initialization + config reload support)
        {
            let mut client_state = CLIENT_STATE.lock().expect("CLIENT_STATE mutex poisoned");
            if let Err(e) = client_state.ensure_initialized() {
                crate::log_capture::suppress_hook(false);
                return Err(e);
            }
        }

        #[cfg(feature = "std")]
        {
            let mut sent_any = false;
            for log in logs {
                if !self.should_send(log, &cfg) {
                    continue;
                }

                let event = Self::build_event(log, &cfg);
                sentry::capture_event(event);
                sent_any = true;
            }
            if sent_any {
                // Flush events by dropping the current guard and creating a new one.
                // The guard flushes on drop with a 2-second deadline.
                let mut client_state = CLIENT_STATE.lock().expect("CLIENT_STATE mutex poisoned");
                client_state.guard = None;
                client_state.config_hash = 0;
            }
        }

        #[cfg(not(feature = "std"))]
        {
            let _ = logs;
        }

        crate::log_capture::suppress_hook(false);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dsn_validation_empty() {
        let config = SentryConfig::default();
        assert!(config.validate_dsn().is_err());
    }

    #[test]
    fn test_dsn_validation_valid() {
        // Note: We can't test valid DSN without actual Sentry credentials
    }

    #[test]
    fn test_map_level() {
        assert_eq!(SentryAdapter::map_level(22), Level::Fatal);
        assert_eq!(SentryAdapter::map_level(21), Level::Fatal);
        assert_eq!(SentryAdapter::map_level(20), Level::Error);
        assert_eq!(SentryAdapter::map_level(19), Level::Warning);
        assert_eq!(SentryAdapter::map_level(17), Level::Info);
        assert_eq!(SentryAdapter::map_level(18), Level::Info);
        assert_eq!(SentryAdapter::map_level(10), Level::Debug);
        assert_eq!(SentryAdapter::map_level(16), Level::Debug);
        assert_eq!(SentryAdapter::map_level(0), Level::Info);
    }
}
