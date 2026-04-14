use super::{Adapter, AdapterError, StreamType};
use crate::log_capture::{CapturedLog, SerializableLog};
use kafka::error::{ErrorKind, KafkaCode};
use kafka::producer::{Producer, Record};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

static KAFKA_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Serialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub timeout_ms: u64,
    pub batch_size: usize,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: String::new(),
            topic: String::new(),
            api_key: None,
            api_secret: None,
            timeout_ms: 5000,
            batch_size: 100,
        }
    }
}

static KAFKA_CONFIG: LazyLock<Mutex<KafkaConfig>> =
    LazyLock::new(|| Mutex::new(KafkaConfig::default()));

pub fn set_kafka_config(config: KafkaConfig) {
    let mut cfg = KAFKA_CONFIG.lock().expect("KAFKA_CONFIG mutex poisoned");
    *cfg = config;
}

pub fn get_kafka_config() -> KafkaConfig {
    let cfg = KAFKA_CONFIG.lock().expect("KAFKA_CONFIG mutex poisoned");
    cfg.clone()
}

pub struct KafkaAdapter {
    enabled: &'static AtomicBool,
}

impl std::fmt::Debug for KafkaAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAdapter")
            .field("enabled", &self.enabled.load(Ordering::SeqCst))
            .finish()
    }
}

impl KafkaAdapter {
    pub fn new() -> Self {
        Self {
            enabled: &KAFKA_ENABLED,
        }
    }

    pub fn set_enabled(enabled: bool) {
        KAFKA_ENABLED.store(enabled, Ordering::SeqCst);
    }

    /// Create a new producer
    fn create_producer(brokers: &[String], timeout_ms: u64) -> Result<Producer, AdapterError> {
        Producer::from_hosts(brokers.to_vec())
            .with_ack_timeout(Duration::from_millis(timeout_ms))
            .create()
            .map_err(|e| AdapterError {
                message: format!("Failed to create Kafka producer: {}", e),
            })
    }

    /// Check if error is UnknownTopicOrPartition
    fn is_unknown_topic_error(err: &kafka::error::Error) -> bool {
        match err.0 {
            ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition) => true,
            ErrorKind::TopicPartitionError(_, _, KafkaCode::UnknownTopicOrPartition) => true,
            _ => false,
        }
    }

    /// Send with retry logic for transient errors like UnknownTopicOrPartition
    /// Returns error if unrecoverable, allowing caller to retry on next poll cycle
    fn send_with_retry(
        producer: &mut Producer,
        record: &Record<'_, (), Vec<u8>>,
        max_retries: u32,
        brokers: &[String],
        timeout_ms: u64,
    ) -> Result<(), AdapterError> {
        let mut last_error = None;

        // Exponential backoff parameters (in milliseconds)
        // These values determine the next retry time the caller should wait
        const UNKNOWN_TOPIC_BASE_DELAY_MS: u64 = 500;
        const UNKNOWN_TOPIC_MAX_DELAY_MS: u64 = 2000;
        const OTHER_ERROR_BASE_DELAY_MS: u64 = 100;
        const OTHER_ERROR_MAX_DELAY_MS: u64 = 1000;
        const BACKOFF_FACTOR: u32 = 2;

        for attempt in 0..=max_retries {
            match producer.send(record) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let is_unknown_topic = Self::is_unknown_topic_error(&e);
                    last_error = Some(e);

                    if attempt < max_retries {
                        // Calculate backoff delay but DON'T block - return error
                        // The background worker will retry on next poll cycle
                        if is_unknown_topic {
                            let backoff = UNKNOWN_TOPIC_BASE_DELAY_MS
                                .saturating_mul(BACKOFF_FACTOR.saturating_pow(attempt) as u64)
                                .min(UNKNOWN_TOPIC_MAX_DELAY_MS);
                            // Recreate producer to force metadata refresh for next attempt
                            if let Ok(new_producer) = Self::create_producer(brokers, timeout_ms) {
                                *producer = new_producer;
                            }
                            // Return error to allow worker to retry on next poll
                            return Err(AdapterError {
                                message: format!(
                                    "Kafka topic not ready (attempt {}/{}), will retry on next poll cycle. Backoff: {}ms. Error: {}",
                                    attempt + 1,
                                    max_retries,
                                    backoff,
                                    last_error.as_ref().map(|e| format!("{}", e)).unwrap_or_default()
                                ),
                            });
                        } else {
                            // Other transient error
                            let backoff = OTHER_ERROR_BASE_DELAY_MS
                                .saturating_mul(BACKOFF_FACTOR.saturating_pow(attempt) as u64)
                                .min(OTHER_ERROR_MAX_DELAY_MS);
                            return Err(AdapterError {
                                message: format!(
                                    "Kafka transient error (attempt {}/{}), will retry on next poll cycle. Backoff: {}ms. Error: {}",
                                    attempt + 1,
                                    max_retries,
                                    backoff,
                                    last_error.as_ref().map(|e| format!("{}", e)).unwrap_or_default()
                                ),
                            });
                        }
                    }
                }
            }
        }

        Err(AdapterError {
            message: format!(
                "Failed to send logs to Kafka after {} retries: {}",
                max_retries,
                last_error
                    .map(|e| format!("{}", e))
                    .unwrap_or_else(|| "Unknown error".to_string())
            ),
        })
    }
}

impl Default for KafkaAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Adapter for KafkaAdapter {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn stream_type(&self) -> StreamType {
        StreamType::Kafka
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

        let config = get_kafka_config();

        if config.brokers.is_empty() || config.topic.is_empty() {
            return Err(AdapterError {
                message: "Kafka brokers or topic not configured".to_string(),
            });
        }

        #[cfg(feature = "std")]
        {
            let brokers: Vec<String> = config
                .brokers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            // Create producer (recreating each time for simplicity and reliability)
            let mut producer = Self::create_producer(&brokers, config.timeout_ms)?;

            for batch in logs.chunks(config.batch_size) {
                let serializable_batch: Vec<SerializableLog> =
                    batch.iter().map(SerializableLog::from).collect();

                let body = serde_json::to_vec(&serializable_batch).map_err(|e| AdapterError {
                    message: format!("Failed to serialize logs: {}", e),
                })?;

                let record = Record::from_value(&config.topic, body);

                // Send with retry logic for UnknownTopicOrPartition errors
                Self::send_with_retry(
                    &mut producer,
                    &record,
                    5, // max_retries - increased for topic auto-creation time
                    &brokers,
                    config.timeout_ms,
                )?;
            }
        }

        #[cfg(not(feature = "std"))]
        {
            let _ = (logs, config);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_default() {
        let config = KafkaConfig::default();
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.batch_size, 100);
        assert!(config.brokers.is_empty());
        assert!(config.topic.is_empty());
    }
}
