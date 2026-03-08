use super::{Adapter, AdapterError, StreamType};
use crate::log_capture::CapturedLog;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

static HTTP_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Serialize)]
pub struct HttpConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub timeout_ms: u64,
    pub batch_size: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            api_key: None,
            timeout_ms: 5000,
            batch_size: 100,
        }
    }
}

static HTTP_CONFIG: LazyLock<Mutex<HttpConfig>> = LazyLock::new(|| Mutex::new(HttpConfig::default()));

pub fn set_http_config(config: HttpConfig) {
    if let Ok(mut cfg) = HTTP_CONFIG.lock() {
        *cfg = config;
    }
}

pub fn get_http_config() -> HttpConfig {
    HTTP_CONFIG
        .lock()
        .map(|cfg| cfg.clone())
        .unwrap_or_default()
}

pub struct HttpAdapter {
    enabled: &'static AtomicBool,
}

impl HttpAdapter {
    pub fn new() -> Self {
        Self {
            enabled: &HTTP_ENABLED,
        }
    }

    pub fn set_enabled(enabled: bool) {
        HTTP_ENABLED.store(enabled, Ordering::SeqCst);
    }
}

impl Default for HttpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Adapter for HttpAdapter {
    fn name(&self) -> &'static str {
        "http"
    }

    fn stream_type(&self) -> StreamType {
        StreamType::Http
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

        let config = get_http_config();

        if config.endpoint.is_empty() {
            return Err(AdapterError {
                message: "HTTP endpoint not configured".to_string(),
            });
        }

        #[cfg(feature = "std")]
        {
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_millis(config.timeout_ms))
                .build()
                .map_err(|e| AdapterError {
                    message: format!("Failed to create HTTP client: {}", e),
                })?;

            // Process logs in batches
            for batch in logs.chunks(config.batch_size) {
                let mut request = client.post(&config.endpoint);

                if let Some(api_key) = &config.api_key {
                    request = request.header("Authorization", format!("Bearer {}", api_key));
                }

                let payload: Vec<serde_json::Value> = batch.iter().map(|log| log.to_json()).collect();

                let body = serde_json::to_vec(&payload).map_err(|e| AdapterError {
                    message: format!("Failed to serialize logs: {}", e),
                })?;

                let response = request
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .map_err(|e| AdapterError {
                        message: format!("Failed to send logs to HTTP endpoint: {}", e),
                    })?;

                if !response.status().is_success() {
                    return Err(AdapterError {
                        message: format!("HTTP request failed with status: {}", response.status()),
                    });
                }
            }
        }

        Ok(())
    }
}

impl CapturedLog {
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::json;

        let level = match self.elevel {
            0..=14 => "DEBUG",
            15..=17 => "INFO",
            18..=24 => "LOG",
            25..=27 => "WARNING",
            28 => "ERROR",
            29 => "FATAL",
            30 => "PANIC",
            _ => "UNKNOWN",
        };

        json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": level,
            "message": self.message,
            "detail": self.detail,
            "hint": self.hint,
            "context": self.context,
            "sqlerrcode": self.sqlerrcode,
            "filename": self.filename,
            "lineno": self.lineno,
            "funcname": self.funcname,
        })
    }
}
