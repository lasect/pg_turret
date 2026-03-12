use super::{Adapter, AdapterError, StreamType};
use crate::log_capture::{CapturedLog, SerializableLog};
use serde::Serialize;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

static HTTP_ENABLED: AtomicBool = AtomicBool::new(false);

static HTTP_CLIENT: LazyLock<Mutex<Option<reqwest::blocking::Client>>> =
    LazyLock::new(|| Mutex::new(None));

fn get_or_create_client(timeout_ms: u64) -> Result<reqwest::blocking::Client, AdapterError> {
    let mut client_guard = HTTP_CLIENT.lock().expect("HTTP_CLIENT mutex poisoned");

    if let Some(ref client) = *client_guard {
        return Ok(client.clone());
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
        .map_err(|e| AdapterError {
            message: format!("Failed to create HTTP client: {}", e),
        })?;

    *client_guard = Some(client.clone());
    Ok(client)
}

pub fn rebuild_http_client() {
    let mut client_guard = HTTP_CLIENT.lock().expect("HTTP_CLIENT mutex poisoned");
    *client_guard = None;
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub timeout_ms: u64,
    pub batch_size: usize,
    pub compression: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            api_key: None,
            timeout_ms: 5000,
            batch_size: 100,
            compression: false,
        }
    }
}

static HTTP_CONFIG: LazyLock<Mutex<HttpConfig>> =
    LazyLock::new(|| Mutex::new(HttpConfig::default()));

pub fn set_http_config(config: HttpConfig) {
    let mut cfg = HTTP_CONFIG.lock().expect("HTTP_CONFIG mutex poisoned");
    let needs_rebuild = cfg.timeout_ms != config.timeout_ms;
    *cfg = config;
    drop(cfg);

    if needs_rebuild {
        rebuild_http_client();
    }
}

pub fn get_http_config() -> HttpConfig {
    let cfg = HTTP_CONFIG.lock().expect("HTTP_CONFIG mutex poisoned");
    cfg.clone()
}

pub struct HttpAdapter {
    enabled: &'static AtomicBool,
}

impl std::fmt::Debug for HttpAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpAdapter")
            .field("enabled", &self.enabled.load(Ordering::SeqCst))
            .finish()
    }
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
            let client = get_or_create_client(config.timeout_ms)?;

            // Process logs in batches
            for batch in logs.chunks(config.batch_size) {
                let mut request = client.post(&config.endpoint);

                if let Some(api_key) = &config.api_key {
                    request = request.header("Authorization", format!("Bearer {}", api_key));
                }

                let serializable_batch: Vec<SerializableLog> =
                    batch.iter().map(SerializableLog::from).collect();

                let body = serde_json::to_vec(&serializable_batch).map_err(|e| AdapterError {
                    message: format!("Failed to serialize logs: {}", e),
                })?;

                // Apply gzip compression if enabled
                if config.compression {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(&body).map_err(|e| AdapterError {
                        message: format!("Failed to compress logs: {}", e),
                    })?;
                    let compressed = encoder.finish().map_err(|e| AdapterError {
                        message: format!("Failed to finish compression: {}", e),
                    })?;

                    request = request
                        .header("Content-Encoding", "gzip")
                        .header("Content-Type", "application/json")
                        .body(compressed);
                } else {
                    request = request
                        .header("Content-Type", "application/json")
                        .body(body);
                }

                let response = request.send().map_err(|e| AdapterError {
                    message: format!("Failed to send logs to HTTP endpoint: {}", e),
                })?;

                if !response.status().is_success() {
                    return Err(AdapterError {
                        message: format!("HTTP request failed with status: {}", response.status()),
                    });
                }
            }
        }

        #[cfg(not(feature = "std"))]
        {
            let _ = (logs, config);
        }

        Ok(())
    }
}
