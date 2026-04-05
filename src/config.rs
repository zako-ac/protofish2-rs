/// Configuration for the Mani (Reliable) transfer subsystem.
///
/// This configuration controls the buffer sizes and channel capacities used
/// by the reliable transfer layer to manage retransmissions, NACKs, and datagram handling.
///
/// # Examples
///
/// ```
/// use protofish2::config::ManiConfig;
///
/// let mut config = ManiConfig::default();
/// config.max_retransmission_buffer_size = 2048; // Store more chunks
/// ```
#[derive(Debug, Clone)]
pub struct ManiConfig {
    /// Maximum number of chunks to keep in the retransmission buffer.
    /// Larger values consume more memory but allow for recovery from longer packet loss windows.
    pub max_retransmission_buffer_size: usize,

    /// Maximum capacity of the NACK (negative acknowledgment) channel.
    /// Controls backpressure when receiver notifies sender of missing chunks.
    pub max_nack_channel_size: usize,

    /// Maximum capacity of the datagram routing channel.
    /// Controls backpressure for incoming datagrams.
    pub max_datagram_channel_size: usize,

    /// Maximum number of chunks in the receive buffer before delivery.
    /// Affects memory usage and delivery latency.
    pub max_chunk_buffer_size: usize,

    /// Duration before a pending chunk that hasn't been matched to a stream is discarded.
    pub pending_chunk_timeout: std::time::Duration,

    /// Interval at which the pending chunk buffer is cleaned up.
    pub pending_chunk_cleanup_interval: std::time::Duration,

    /// Initial number of backpressure credits granted to the sender.
    pub initial_backpressure_credits: usize,

    /// Increment of backpressure credits for receiver to send CreditUpdate
    pub backpressure_credit_batch_size: usize,
}

impl Default for ManiConfig {
    fn default() -> Self {
        Self {
            max_retransmission_buffer_size: 1024,
            max_nack_channel_size: 100,
            max_datagram_channel_size: 1000,
            max_chunk_buffer_size: 1000,
            pending_chunk_timeout: std::time::Duration::from_secs(5),
            pending_chunk_cleanup_interval: std::time::Duration::from_secs(1),
            initial_backpressure_credits: 100,
            backpressure_credit_batch_size: 10,
        }
    }
}

/// Main configuration for the Protofish2 protocol.
///
/// This configuration is provided when establishing connections and controls
/// behavior of the entire protocol stack.
///
/// # Examples
///
/// ```
/// use protofish2::config::ProtofishConfig;
///
/// let config = ProtofishConfig::default();
/// // Customize as needed
/// ```
#[derive(Debug, Clone)]
pub struct ProtofishConfig {
    /// Retransmission buffer size for sender-side chunks.
    pub retransmission_buffer_size: usize,

    /// Configuration for the Mani reliable transfer layer.
    pub mani_config: ManiConfig,
}

/// Configuration for automatic reconnection behavior.
///
/// Controls how the `ReconnectingConnection` handles disconnects and failed
/// connection attempts.
///
/// # Examples
///
/// ```
/// use protofish2::config::ReconnectConfig;
/// use std::time::Duration;
///
/// let config = ReconnectConfig {
///     initial_backoff: Duration::from_millis(500),
///     max_backoff: Duration::from_secs(30),
///     backoff_multiplier: 1.5,
///     max_retries: Some(5),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// The initial delay before the first retry attempt.
    pub initial_backoff: std::time::Duration,

    /// The maximum delay between retry attempts.
    pub max_backoff: std::time::Duration,

    /// The multiplier applied to the delay after each failed attempt.
    pub backoff_multiplier: f64,

    /// The maximum number of consecutive failed retry attempts before giving up.
    /// `None` indicates infinite retries.
    pub max_retries: Option<usize>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_backoff: std::time::Duration::from_millis(500),
            max_backoff: std::time::Duration::from_secs(30),
            backoff_multiplier: 1.5,
            max_retries: None,
        }
    }
}

impl Default for ProtofishConfig {
    fn default() -> Self {
        Self {
            retransmission_buffer_size: 1024,
            mani_config: ManiConfig::default(),
        }
    }
}
