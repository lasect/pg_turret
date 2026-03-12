use crate::log_capture;
use pgrx::prelude::*;

pub fn init_metrics() {
    // Metrics are tracked via atomic counters
    // The pg_stat view will be registered separately
}

#[pg_extern]
pub fn get_metrics() -> TableIterator<'static, (name!(name, String), name!(value, i64))> {
    TableIterator::new(vec![
        (
            "logs_captured".to_string(),
            log_capture::get_logs_captured() as i64,
        ),
        (
            "logs_dropped".to_string(),
            log_capture::get_logs_dropped() as i64,
        ),
        ("logs_sent".to_string(), log_capture::get_logs_sent() as i64),
        (
            "logs_retry_failed".to_string(),
            log_capture::get_logs_retry_failed() as i64,
        ),
        (
            "logs_pending".to_string(),
            log_capture::get_pending_count() as i64,
        ),
        (
            "ring_buffer_capacity".to_string(),
            log_capture::get_buffer_capacity() as i64,
        ),
    ])
}

#[pg_extern]
pub fn get_logs_captured_count() -> i64 {
    log_capture::get_logs_captured() as i64
}

#[pg_extern]
pub fn get_logs_dropped_count() -> i64 {
    log_capture::get_logs_dropped() as i64
}

#[pg_extern]
pub fn get_logs_sent_count() -> i64 {
    log_capture::get_logs_sent() as i64
}

#[pg_extern]
pub fn get_logs_retry_failed_count() -> i64 {
    log_capture::get_logs_retry_failed() as i64
}
