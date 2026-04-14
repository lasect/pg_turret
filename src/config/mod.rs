use std::sync::{LazyLock, Mutex};

pub mod http;
pub mod kafka;
pub mod sentry;

static ENABLED_STREAMS: LazyLock<Mutex<StreamFlags>> =
    LazyLock::new(|| Mutex::new(StreamFlags::default()));

#[derive(Debug, Clone, Default)]
pub struct StreamFlags {
    pub http: bool,
    pub sentry: bool,
    pub axiom: bool,
    pub datadog: bool,
    pub s3: bool,
    pub kafka: bool,
}

impl StreamFlags {
    pub fn is_enabled(&self, stream: StreamType) -> bool {
        match stream {
            StreamType::Http => self.http,
            StreamType::Sentry => self.sentry,
            StreamType::Axiom => self.axiom,
            StreamType::Datadog => self.datadog,
            StreamType::S3 => self.s3,
            StreamType::Kafka => self.kafka,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamType {
    Http,
    Sentry,
    Axiom,
    Datadog,
    S3,
    Kafka,
}

impl StreamType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "http" => Some(StreamType::Http),
            "sentry" => Some(StreamType::Sentry),
            "axiom" => Some(StreamType::Axiom),
            "datadog" => Some(StreamType::Datadog),
            "s3" => Some(StreamType::S3),
            "kafka" => Some(StreamType::Kafka),
            _ => None,
        }
    }
}

pub trait Adapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn stream_type(&self) -> StreamType;
    fn is_enabled(&self) -> bool;
    fn send(&self, logs: &[crate::log_capture::CapturedLog]) -> Result<(), AdapterError>;
}

#[derive(Debug, Clone)]
pub struct AdapterError {
    pub message: String,
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AdapterError {}

pub fn set_stream_enabled(stream: StreamType, enabled: bool) {
    let mut flags = ENABLED_STREAMS
        .lock()
        .expect("ENABLED_STREAMS mutex poisoned");
    match stream {
        StreamType::Http => flags.http = enabled,
        StreamType::Sentry => flags.sentry = enabled,
        StreamType::Axiom => flags.axiom = enabled,
        StreamType::Datadog => flags.datadog = enabled,
        StreamType::S3 => flags.s3 = enabled,
        StreamType::Kafka => flags.kafka = enabled,
    }
}

pub fn is_stream_enabled(stream: StreamType) -> bool {
    let flags = ENABLED_STREAMS
        .lock()
        .expect("ENABLED_STREAMS mutex poisoned");
    flags.is_enabled(stream)
}

pub fn get_enabled_streams() -> Vec<StreamType> {
    let flags = ENABLED_STREAMS
        .lock()
        .expect("ENABLED_STREAMS mutex poisoned");
    let mut enabled = Vec::new();
    if flags.http {
        enabled.push(StreamType::Http);
    }
    if flags.sentry {
        enabled.push(StreamType::Sentry);
    }
    if flags.axiom {
        enabled.push(StreamType::Axiom);
    }
    if flags.datadog {
        enabled.push(StreamType::Datadog);
    }
    if flags.s3 {
        enabled.push(StreamType::S3);
    }
    if flags.kafka {
        enabled.push(StreamType::Kafka);
    }
    enabled
}

pub fn reload_config() {
    #[cfg(feature = "std")]
    {
        set_stream_enabled(StreamType::Http, crate::HTTP_ENABLED.get());
        set_stream_enabled(StreamType::Kafka, crate::KAFKA_ENABLED.get());
        set_stream_enabled(StreamType::Sentry, crate::SENTRY_ENABLED.get());

        // Other streams will be added here as they are implemented
    }
}
