use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::config::ReconnectConfig;
use crate::connection::api::{ProtofishClient, ProtofishConnection, ProtofishConnectionError};
use crate::mani::stream::ManiStream;

/// A resilient wrapper around `ProtofishConnection` that automatically handles reconnects.
///
/// If a connection drops or a stream operation fails due to connection issues,
/// this struct will transparently attempt to re-establish the connection based on
/// the provided `ReconnectConfig` backoff strategy.
///
/// # Examples
///
/// ```ignore
/// let config = ReconnectConfig::default();
/// let client = Arc::new(ProtofishClient::bind(client_config)?);
///
/// let mut conn = ReconnectingConnection::connect(
///     client,
///     server_addr,
///     "localhost".to_string(),
///     HashMap::new(),
///     config,
/// ).await?;
///
/// let stream = conn.open_mani().await?; // Will retry internally if it fails
/// ```
pub struct ReconnectingConnection {
    client: Arc<ProtofishClient>,
    server_addr: SocketAddr,
    server_name: String,
    headers: HashMap<String, Bytes>,
    config: ReconnectConfig,
    active_connection: Option<ProtofishConnection>,
}

impl ReconnectingConnection {
    /// Creates a new `ReconnectingConnection` and establishes the initial connection.
    ///
    /// If the initial connection fails, it will immediately enter the retry backoff loop.
    pub async fn connect(
        client: Arc<ProtofishClient>,
        server_addr: SocketAddr,
        server_name: String,
        headers: HashMap<String, Bytes>,
        config: ReconnectConfig,
    ) -> Result<Self, ProtofishConnectionError> {
        let mut rc = Self {
            client,
            server_addr,
            server_name,
            headers,
            config,
            active_connection: None,
        };

        rc.ensure_connected().await?;
        Ok(rc)
    }

    /// Internal helper to ensure we have a valid connection, attempting to reconnect if needed.
    async fn ensure_connected(&mut self) -> Result<(), ProtofishConnectionError> {
        if self.active_connection.is_some() {
            return Ok(());
        }

        let mut current_backoff = self.config.initial_backoff;
        let mut retries = 0;

        loop {
            if self.config.max_retries.is_some_and(|max| retries > max) {
                return Err(ProtofishConnectionError::MaxRetriesExceeded);
            }

            match self
                .client
                .connect(self.server_addr, &self.server_name, self.headers.clone())
                .await
            {
                Ok(conn) => {
                    self.active_connection = Some(conn);
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        "Connection failed: {}. Retrying in {:?} (Attempt {})",
                        e,
                        current_backoff,
                        retries + 1
                    );
                    tokio::time::sleep(current_backoff).await;

                    retries += 1;
                    current_backoff = std::cmp::min(
                        Duration::from_secs_f64(
                            current_backoff.as_secs_f64() * self.config.backoff_multiplier,
                        ),
                        self.config.max_backoff,
                    );
                }
            }
        }
    }

    /// Opens a new bidirectional stream for sending data.
    ///
    /// Transparently handles disconnects by reconnecting and retrying the operation.
    pub async fn open_mani(&mut self) -> Result<ManiStream, ProtofishConnectionError> {
        loop {
            self.ensure_connected().await?;

            if let Some(conn) = &mut self.active_connection {
                match conn.open_mani().await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::warn!("Failed to open stream, dropping connection: {}", e);
                        self.active_connection = None; // Force a reconnect on the next loop iteration
                    }
                }
            }
        }
    }

    /// Accepts an incoming stream opened by the peer.
    ///
    /// Transparently handles disconnects by reconnecting and waiting for a new incoming stream.
    pub async fn accept_mani(&mut self) -> Result<ManiStream, ProtofishConnectionError> {
        loop {
            self.ensure_connected().await?;

            if let Some(conn) = &mut self.active_connection {
                match conn.accept_mani().await {
                    Ok(stream) => return Ok(stream),
                    Err(e) => {
                        tracing::warn!("Failed to accept stream, dropping connection: {}", e);
                        self.active_connection = None; // Force a reconnect on the next loop iteration
                    }
                }
            }
        }
    }

    /// Access the underlying `ProtofishConnection` if connected.
    pub fn get_connection(&self) -> Option<&ProtofishConnection> {
        self.active_connection.as_ref()
    }
}
