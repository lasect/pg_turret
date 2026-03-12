use std::sync::{LazyLock, Mutex};

pub mod http;

static ENABLED_STREAMS: LazyLock<Mutex<StreamFlags>> =
    LazyLock::new(|| Mutex::new(StreamFlags::default()));

#[derive(Debug, Clone, Default)]
pub struct StreamFlags {
    pub http: bool,
    pub sentry: bool,
    pub axiom: bool,
    pub datadog: bool,
    pub websockets: bool,
    pub s3: bool,
}

impl StreamFlags {
    pub fn is_enabled(&self, stream: StreamType) -> bool {
        match stream {
            StreamType::Http => self.http,
            StreamType::Sentry => self.sentry,
            StreamType::Axiom => self.axiom,
            StreamType::Datadog => self.datadog,
            StreamType::WebSockets => self.websockets,
            StreamType::S3 => self.s3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StreamType {
    Http,
    Sentry,
    Axiom,
    Datadog,
    WebSockets,
    S3,
}

impl StreamType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "http" => Some(StreamType::Http),
            "sentry" => Some(StreamType::Sentry),
            "axiom" => Some(StreamType::Axiom),
            "datadog" => Some(StreamType::Datadog),
            "websockets" | "ws" => Some(StreamType::WebSockets),
            "s3" => Some(StreamType::S3),
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
    if let Ok(mut flags) = ENABLED_STREAMS.lock() {
        match stream {
            StreamType::Http => flags.http = enabled,
            StreamType::Sentry => flags.sentry = enabled,
            StreamType::Axiom => flags.axiom = enabled,
            StreamType::Datadog => flags.datadog = enabled,
            StreamType::WebSockets => flags.websockets = enabled,
            StreamType::S3 => flags.s3 = enabled,
        }
    }
}

pub fn is_stream_enabled(stream: StreamType) -> bool {
    ENABLED_STREAMS
        .lock()
        .map(|flags| flags.is_enabled(stream))
        .unwrap_or(false)
}

pub fn get_enabled_streams() -> Vec<StreamType> {
    ENABLED_STREAMS
        .lock()
        .map(|flags| {
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
            if flags.websockets {
                enabled.push(StreamType::WebSockets);
            }
            if flags.s3 {
                enabled.push(StreamType::S3);
            }
            enabled
        })
        .unwrap_or_default()
}

pub fn reload_config() {
    #[cfg(feature = "std")]
    {
        set_stream_enabled(StreamType::Http, crate::HTTP_ENABLED.get());

        // Other streams will be added here as they are implemented
    }
}
