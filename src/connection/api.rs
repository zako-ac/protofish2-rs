use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use quinn::Endpoint;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::compression::CompressionType;
use crate::connection::codec::ControlStreamCodec;
use crate::connection::message::{ClientHello, ConnectionMessage, ServerHello};

/// Errors that can occur during connection establishment and management.
///
/// This error type covers all connection-level issues including handshake failures,
/// QUIC protocol errors, and I/O errors.
#[derive(Error, Debug)]
pub enum ProtofishConnectionError {
    /// An error occurred in the underlying QUIC connection.
    #[error("Quic connection error: {0}")]
    QuicError(#[from] quinn::ConnectionError),

    /// Failed to create or configure the endpoint.
    #[error("Endpoint error: {0}")]
    EndpointError(String),

    /// The handshake between client and server failed.
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    /// A system I/O error occurred.
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    /// Failed to write to the QUIC stream.
    #[error("Write Error")]
    WriteError(#[from] quinn::WriteError),

    /// Failed to read from the QUIC stream.
    #[error("Read Error")]
    ReadError(#[from] quinn::ReadError),

    /// The client exceeded the maximum number of connection retries.
    #[error("Max retries exceeded")]
    MaxRetriesExceeded,

    /// The handshake did not complete within the configured timeout.
    #[error("Handshake timed out")]
    HandshakeTimeout,
}

/// Configuration for a Protofish2 server.
///
/// # Examples
///
/// ```ignore
/// use protofish2::connection::ServerConfig;
/// use rustls::pki_types::{CertificateDer, PrivateKeyDer};
/// use std::net::SocketAddr;
/// use std::time::Duration;
/// use protofish2::compression::CompressionType;
///
/// let config = ServerConfig {
///     bind_address: "0.0.0.0:5000".parse().unwrap(),
///     cert_chain: vec![/* load certificates */],
///     private_key: /* load private key */,
///     supported_compression_types: vec![CompressionType::None],
///     keepalive_interval: Duration::from_secs(30),
///     protofish_config: Default::default(),
/// };
/// ```
pub struct ServerConfig {
    /// Address to bind the server to (e.g., "0.0.0.0:5000")
    pub bind_address: SocketAddr,

    /// TLS certificate chain for the server
    pub cert_chain: Vec<CertificateDer<'static>>,

    /// Private key corresponding to the certificate
    pub private_key: PrivateKeyDer<'static>,

    /// List of compression algorithms the server supports
    pub supported_compression_types: Vec<CompressionType>,

    /// How often to send keepalive packets
    pub keepalive_interval: Duration,

    /// Protocol configuration for Protofish2
    pub protofish_config: crate::config::ProtofishConfig,
}

/// A Protofish2 server that accepts incoming connections.
///
/// # Examples
///
/// ```ignore
/// let config = ServerConfig { /* ... */ };
/// let server = ProtofishServer::bind(config)?;
/// let addr = server.local_addr()?;
/// println!("Server listening on {}", addr);
///
/// loop {
///     if let Some(incoming) = server.accept().await {
///         let conn = incoming.accept().await?;
///         // Handle connection...
///     }
/// }
/// ```
pub struct ProtofishServer {
    endpoint: Endpoint,
    config: Arc<ServerConfig>,
}

impl ProtofishServer {
    /// Creates and binds a new Protofish2 server.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ServerConfig { /* ... */ };
    /// let server = ProtofishServer::bind(config)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ProtofishConnectionError::EndpointError` if the endpoint cannot be created
    /// or if TLS configuration fails.
    pub fn bind(config: ServerConfig) -> Result<Self, ProtofishConnectionError> {
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.cert_chain.clone(), config.private_key.clone_key())
            .map_err(|e| {
                ProtofishConnectionError::EndpointError(format!(
                    "Failed to load server TLS certificates: {}",
                    e
                ))
            })?;

        server_crypto.alpn_protocols = vec![b"protofish2".to_vec()];

        let mut server_config = quinn::ServerConfig::with_crypto(std::sync::Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).map_err(|e| {
                ProtofishConnectionError::EndpointError(format!(
                    "Failed to build QUIC server crypto config: {}",
                    e
                ))
            })?,
        ));

        let quic_idle_timeout = config.protofish_config.keepalive_timeout * 3;
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(quic_idle_timeout)
                .unwrap_or(quinn::IdleTimeout::from(quinn::VarInt::from_u32(u32::MAX))),
        ));
        server_config.transport_config(std::sync::Arc::new(transport));

        let endpoint =
            quinn::Endpoint::server(server_config, config.bind_address).map_err(|e| {
                ProtofishConnectionError::EndpointError(format!(
                    "Failed to bind server endpoint to {}: {}",
                    config.bind_address, e
                ))
            })?;

        Ok(Self {
            endpoint,
            config: Arc::new(config),
        })
    }

    /// Returns the local address the server is bound to.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let addr = server.local_addr()?;
    /// println!("Listening on {}", addr);
    /// ```
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// Accepts an incoming connection attempt.
    ///
    /// Blocks until a new connection arrives, then returns an `IncomingProtofishConnection`
    /// that must be accepted to complete the handshake.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// loop {
    ///     if let Some(incoming) = server.accept().await {
    ///         let conn = incoming.accept().await?;
    ///         // Handle connection
    ///     }
    /// }
    /// ```
    pub async fn accept(&self) -> Option<IncomingProtofishConnection> {
        let incoming = self.endpoint.accept().await?;
        Some(IncomingProtofishConnection {
            incoming,
            server_config: self.config.clone(),
        })
    }

    /// Gracefully closes the server endpoint and all its connections.
    ///
    /// The `error_code` and `reason` are sent to connected clients.
    pub fn close(&self, error_code: u32, reason: &[u8]) {
        self.endpoint.close(error_code.into(), reason);
    }
}

/// An incoming connection attempt awaiting acceptance.
///
/// This represents a QUIC connection that has been established but the
/// Protofish2 handshake has not yet been completed. Call `accept()` to
/// complete the handshake.
pub struct IncomingProtofishConnection {
    incoming: quinn::Incoming,
    server_config: Arc<ServerConfig>,
}

impl IncomingProtofishConnection {
    /// Returns the remote address of the incoming connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.incoming.remote_address()
    }

    /// Completes the Protofish2 handshake and returns an established connection.
    ///
    /// This negotiates compression type and other connection parameters with the client.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let incoming = server.accept().await.unwrap();
    /// println!("Connection from {}", incoming.remote_addr());
    /// let conn = incoming.accept().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ProtofishConnectionError::HandshakeFailed` if the client sends invalid
    /// handshake messages or uses an unsupported protocol version.
    pub async fn accept(self) -> Result<ProtofishConnection, ProtofishConnectionError> {
        let server_config = self.server_config;
        let handshake_timeout = server_config.protofish_config.handshake_timeout;
        let keepalive_timeout = server_config.protofish_config.keepalive_timeout;
        let incoming = self.incoming;

        let (conn, compression_type, keepalive_interval, framed_send, framed_recv) =
            tokio::time::timeout(handshake_timeout, async {
                let conn = incoming.await?;

                let (send, recv) = conn.accept_bi().await?;

                let mut framed_recv =
                    tokio_util::codec::FramedRead::new(recv, ControlStreamCodec::new());
                let mut framed_send =
                    tokio_util::codec::FramedWrite::new(send, ControlStreamCodec::new());

                let hello_msg = framed_recv.next().await.ok_or_else(|| {
                    ProtofishConnectionError::HandshakeFailed(
                        "Connection closed before ClientHello".into(),
                    )
                })??;

                let client_hello = match hello_msg {
                    ConnectionMessage::ClientHello(hello) => hello,
                    _ => {
                        return Err(ProtofishConnectionError::HandshakeFailed(
                            "Expected ClientHello".into(),
                        ));
                    }
                };

                if client_hello.version != 1 {
                    return Err(ProtofishConnectionError::HandshakeFailed(format!(
                        "Unsupported version {}",
                        client_hello.version
                    )));
                }

                let compression_type = server_config
                    .supported_compression_types
                    .iter()
                    .find(|&&c| client_hello.available_compression_types.contains(&c))
                    .copied()
                    .unwrap_or(CompressionType::None);

                let server_hello = ServerHello {
                    version: 1,
                    headers: HashMap::new(),
                    compression_type,
                    keepalive_interval_ms: server_config.keepalive_interval.as_millis() as u32,
                };

                let keepalive_interval =
                    Duration::from_millis(server_hello.keepalive_interval_ms as u64);

                framed_send
                    .send(ConnectionMessage::ServerHello(server_hello))
                    .await?;

                Ok::<_, ProtofishConnectionError>((
                    conn,
                    compression_type,
                    keepalive_interval,
                    framed_send,
                    framed_recv,
                ))
            })
            .await
            .map_err(|_| ProtofishConnectionError::HandshakeTimeout)??;

        let datagram_router = crate::datagram::router::DatagramPacketRouter::new(
            server_config
                .protofish_config
                .mani_config
                .max_chunk_buffer_size,
            server_config
                .protofish_config
                .mani_config
                .pending_chunk_timeout,
            server_config
                .protofish_config
                .mani_config
                .pending_chunk_cleanup_interval,
        );
        let router_clone = datagram_router.clone();
        let quic_clone = conn.clone();
        tokio::spawn(async move {
            router_clone.run(quic_clone).await;
        });

        let quic_clone_ka = conn.clone();
        let keepalive_abort = tokio::spawn(async move {
            keepalive_task(
                framed_send,
                framed_recv,
                keepalive_interval,
                keepalive_timeout.max(keepalive_interval),
                quic_clone_ka,
            )
            .await;
        })
        .abort_handle();

        Ok(ProtofishConnection {
            quic_conn: conn,
            state: Arc::new(RwLock::new(ConnectionState { compression_type })),
            datagram_router,
            protofish_config: server_config.protofish_config.clone(),
            keepalive_abort: if keepalive_interval.is_zero() { None } else { Some(keepalive_abort) },
        })
    }
}

/// Configuration for a Protofish2 client.
///
/// # Examples
///
/// ```ignore
/// use protofish2::connection::ClientConfig;
/// use std::net::SocketAddr;
/// use std::time::Duration;
/// use protofish2::compression::CompressionType;
///
/// let config = ClientConfig {
///     bind_address: "0.0.0.0:0".parse().unwrap(),
///     root_certificates: vec![/* load certificates */],
///     supported_compression_types: vec![CompressionType::None],
///     keepalive_range: Duration::from_secs(10)..Duration::from_secs(30),
///     protofish_config: Default::default(),
/// };
/// ```
pub struct ClientConfig {
    /// Local address to bind from (use 0.0.0.0:0 to let OS choose)
    pub bind_address: SocketAddr,

    /// Root CA certificates for verifying server certificates
    pub root_certificates: Vec<CertificateDer<'static>>,

    /// List of compression algorithms the client supports
    pub supported_compression_types: Vec<CompressionType>,

    /// Acceptable range for keepalive interval negotiation (min..max)
    pub keepalive_range: Range<Duration>,

    /// Protocol configuration for Protofish2
    pub protofish_config: crate::config::ProtofishConfig,
}

/// A Protofish2 client for connecting to servers.
///
/// # Examples
///
/// ```ignore
/// let config = ClientConfig { /* ... */ };
/// let client = ProtofishClient::bind(config)?;
/// let mut conn = client.connect(
///     "example.com:5000".parse().unwrap(),
///     "example.com",
///     HashMap::new(),
/// ).await?;
/// ```
pub struct ProtofishClient {
    endpoint: Endpoint,
    config: Arc<ClientConfig>,
}

impl ProtofishClient {
    /// Creates and binds a new Protofish2 client.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ClientConfig { /* ... */ };
    /// let client = ProtofishClient::bind(config)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ProtofishConnectionError::EndpointError` if the endpoint cannot be created
    /// or if TLS configuration fails.
    pub fn bind(config: ClientConfig) -> Result<Self, ProtofishConnectionError> {
        let mut root_store = rustls::RootCertStore::empty();
        for cert in &config.root_certificates {
            root_store.add(cert.clone()).map_err(|e| {
                ProtofishConnectionError::EndpointError(format!(
                    "Failed to load root certificate: {}",
                    e
                ))
            })?;
        }

        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        client_crypto.alpn_protocols = vec![b"protofish2".to_vec()];

        let mut client_config = quinn::ClientConfig::new(std::sync::Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).map_err(|e| {
                ProtofishConnectionError::EndpointError(format!(
                    "Failed to build QUIC client crypto config: {}",
                    e
                ))
            })?,
        ));

        let quic_idle_timeout = config.protofish_config.keepalive_timeout * 3;
        let mut transport = quinn::TransportConfig::default();

        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(quic_idle_timeout)
                .unwrap_or(quinn::IdleTimeout::from(quinn::VarInt::from_u32(u32::MAX))),
        ));
        client_config.transport_config(std::sync::Arc::new(transport));

        let mut endpoint = quinn::Endpoint::client(config.bind_address).map_err(|e| {
            ProtofishConnectionError::EndpointError(format!(
                "Failed to bind client endpoint to {}: {}",
                config.bind_address, e
            ))
        })?;

        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            config: Arc::new(config),
        })
    }

    /// Connects to a Protofish2 server.
    ///
    /// # Arguments
    ///
    /// * `server_addr` - Address of the server to connect to
    /// * `server_name` - SNI hostname for TLS verification
    /// * `headers` - Optional custom headers to send during handshake
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut conn = client.connect(
    ///     "example.com:5000".parse().unwrap(),
    ///     "example.com",
    ///     HashMap::new(),
    /// ).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ProtofishConnectionError::HandshakeFailed` if the server rejects the
    /// handshake or sends invalid messages.
    pub async fn connect(
        &self,
        server_addr: SocketAddr,
        server_name: &str,
        headers: HashMap<String, Bytes>,
    ) -> Result<ProtofishConnection, ProtofishConnectionError> {
        let handshake_timeout = self.config.protofish_config.handshake_timeout;
        let keepalive_timeout = self.config.protofish_config.keepalive_timeout;

        let (conn, compression_type, keepalive_interval, framed_send, framed_recv) =
            tokio::time::timeout(handshake_timeout, async {
                let conn = self
                    .endpoint
                    .connect(server_addr, server_name)
                    .map_err(|e| {
                        ProtofishConnectionError::EndpointError(format!(
                            "Failed to connect to server '{}' at {}: {}",
                            server_name, server_addr, e
                        ))
                    })?
                    .await?;

                let (send, recv) = conn.open_bi().await?;

                let mut framed_recv =
                    tokio_util::codec::FramedRead::new(recv, ControlStreamCodec::new());
                let mut framed_send =
                    tokio_util::codec::FramedWrite::new(send, ControlStreamCodec::new());

                let client_hello = ClientHello {
                    version: 1,
                    host: server_name.to_string(),
                    headers,
                    available_compression_types: self.config.supported_compression_types.clone(),
                    keepalive_min_ms: self.config.keepalive_range.start.as_millis() as u32,
                    keepalive_max_ms: self.config.keepalive_range.end.as_millis() as u32,
                };

                framed_send
                    .send(ConnectionMessage::ClientHello(client_hello))
                    .await?;

                let hello_resp = framed_recv.next().await.ok_or_else(|| {
                    ProtofishConnectionError::HandshakeFailed(
                        "Connection closed before ServerHello".into(),
                    )
                })??;

                let server_hello = match hello_resp {
                    ConnectionMessage::ServerHello(hello) => hello,
                    _ => {
                        return Err(ProtofishConnectionError::HandshakeFailed(
                            "Expected ServerHello".into(),
                        ));
                    }
                };

                let compression_type = server_hello.compression_type;
                let keepalive_interval =
                    Duration::from_millis(server_hello.keepalive_interval_ms as u64);

                Ok::<_, ProtofishConnectionError>((
                    conn,
                    compression_type,
                    keepalive_interval,
                    framed_send,
                    framed_recv,
                ))
            })
            .await
            .map_err(|_| ProtofishConnectionError::HandshakeTimeout)??;

        let datagram_router = crate::datagram::router::DatagramPacketRouter::new(
            self.config
                .protofish_config
                .mani_config
                .max_chunk_buffer_size,
            self.config
                .protofish_config
                .mani_config
                .pending_chunk_timeout,
            self.config
                .protofish_config
                .mani_config
                .pending_chunk_cleanup_interval,
        );
        let router_clone = datagram_router.clone();
        let quic_clone = conn.clone();
        tokio::spawn(async move {
            router_clone.run(quic_clone).await;
        });

        let quic_clone_ka = conn.clone();
        let keepalive_abort = tokio::spawn(async move {
            keepalive_task(
                framed_send,
                framed_recv,
                keepalive_interval,
                keepalive_timeout.max(keepalive_interval),
                quic_clone_ka,
            )
            .await;
        })
        .abort_handle();

        Ok(ProtofishConnection {
            quic_conn: conn,
            state: Arc::new(RwLock::new(ConnectionState { compression_type })),
            datagram_router,
            protofish_config: self.config.protofish_config.clone(),
            keepalive_abort: if keepalive_interval.is_zero() { None } else { Some(keepalive_abort) },
        })
    }
}

/// State associated with an established Protofish2 connection.
///
/// This is internal state managed by the connection.
#[doc(hidden)]
pub struct ConnectionState {
    pub compression_type: CompressionType,
}

/// An established Protofish2 connection.
///
/// This represents a fully negotiated connection ready for opening/accepting streams.
/// Use `open_mani()` to create a stream for sending, or `accept_mani()` to receive incoming streams.
///
/// # Examples
///
/// ```ignore
/// let mut conn = client.connect(...).await?;
///
/// let mut stream = conn.open_mani().await?;
/// let (send, recv) = stream.start_transfer(TransferMode::Dual, CompressionType::None, SequenceNumber(0), None).await?;
/// send.send(Timestamp(0), Bytes::from("Hello")).await?;
/// send.end().await?;
/// ```
pub struct ProtofishConnection {
    pub quic_conn: quinn::Connection,
    pub state: Arc<RwLock<ConnectionState>>,
    pub datagram_router: crate::datagram::router::DatagramPacketRouter,
    pub protofish_config: crate::config::ProtofishConfig,
    pub keepalive_abort: Option<tokio::task::AbortHandle>,
}

impl ProtofishConnection {
    /// Opens a new bidirectional stream for sending data.
    ///
    /// Use this to initiate a new transfer where you will be sending data to the peer.
    /// After opening, call `start_transfer()` on the stream to begin the transfer.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut stream = conn.open_mani().await?;
    /// let send = stream.start_transfer(TransferMode::Dual, CompressionType::None, SequenceNumber(0), None).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying QUIC connection fails to open a stream.
    pub async fn open_mani(
        &self,
    ) -> Result<crate::mani::stream::ManiStream, ProtofishConnectionError> {
        let (send, recv) = self.quic_conn.open_bi().await?;
        let id = crate::ManiStreamId(send.id().index());

        let writer = crate::mani::frame::ManiWriteFrame::new(send);
        let reader = crate::mani::frame::ManiReadFrame::new(id, recv);

        let (stream, task) = crate::mani::stream::ManiStream::pair(
            id,
            self.quic_conn.clone(),
            writer,
            reader,
            self.datagram_router.clone(),
            self.protofish_config
                .mani_config
                .max_retransmission_buffer_size,
            self.protofish_config.mani_config.max_nack_channel_size,
            self.protofish_config.mani_config.max_datagram_channel_size,
            self.protofish_config.mani_config.max_chunk_buffer_size,
            self.protofish_config
                .mani_config
                .initial_backpressure_credits,
            self.protofish_config
                .mani_config
                .backpressure_credit_batch_size,
            self.protofish_config.mani_config.max_datagram_size,
        );

        tokio::spawn(async move {
            task.run().await;
        });

        Ok(stream)
    }

    /// Accepts an incoming stream opened by the peer.
    ///
    /// Blocks until the peer opens a new stream. Use this to receive incoming transfers.
    /// After accepting, the stream will contain either data payload or a transfer request.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let stream = conn.accept_mani().await?;
    /// match stream.recv().await? {
    ///     ManiRecvMessage::Transfer(reliable, unreliable) => {
    ///         // Handle incoming transfer
    ///     }
    ///     ManiRecvMessage::Payload(data) => {
    ///         // Handle payload
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying QUIC connection fails.
    pub async fn accept_mani(
        &self,
    ) -> Result<crate::mani::stream::ManiStream, ProtofishConnectionError> {
        let (send, recv) = self.quic_conn.accept_bi().await?;
        let id = crate::ManiStreamId(send.id().index());

        let writer = crate::mani::frame::ManiWriteFrame::new(send);
        let reader = crate::mani::frame::ManiReadFrame::new(id, recv);

        let (stream, task) = crate::mani::stream::ManiStream::pair(
            id,
            self.quic_conn.clone(),
            writer,
            reader,
            self.datagram_router.clone(),
            self.protofish_config
                .mani_config
                .max_retransmission_buffer_size,
            self.protofish_config.mani_config.max_nack_channel_size,
            self.protofish_config.mani_config.max_datagram_channel_size,
            self.protofish_config.mani_config.max_chunk_buffer_size,
            self.protofish_config
                .mani_config
                .initial_backpressure_credits,
            self.protofish_config
                .mani_config
                .backpressure_credit_batch_size,
            self.protofish_config.mani_config.max_datagram_size,
        );

        tokio::spawn(async move {
            task.run().await;
        });

        Ok(stream)
    }

    /// Closes the connection gracefully.
    pub fn close(&self) {
        self.quic_conn.close(quinn::VarInt::from_u32(0), b"");
    }
}

impl Drop for ProtofishConnection {
    fn drop(&mut self) {
        if let Some(handle) = &self.keepalive_abort {
            handle.abort();
        }
    }
}

async fn keepalive_task(
    mut send: tokio_util::codec::FramedWrite<quinn::SendStream, ControlStreamCodec>,
    mut recv: tokio_util::codec::FramedRead<quinn::RecvStream, ControlStreamCodec>,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
    conn: quinn::Connection,
) {
    if keepalive_interval.is_zero() {
        return;
    }

    let mut timer = tokio::time::interval(keepalive_interval);
    timer.tick().await; // Initial tick
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = timer.tick() => {
                tracing::trace!("Keepalive tick");

                if last_activity.elapsed() > keepalive_timeout {
                    conn.close(quinn::VarInt::from_u32(1), b"keepalive timeout");
                    break;
                }
                if send.send(ConnectionMessage::Keepalive).await.is_err() {
                    break;
                }
            }
            msg = recv.next() => {
                match msg {
                    Some(Ok(ConnectionMessage::Keepalive)) => {
                        tracing::trace!("Received keepalive");

                        last_activity = tokio::time::Instant::now();
                        if send.send(ConnectionMessage::KeepaliveAck).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(ConnectionMessage::KeepaliveAck)) => {
                        tracing::trace!("Received keepalive ack");
                        last_activity = tokio::time::Instant::now();
                    }
                    Some(Ok(ConnectionMessage::Close)) => {
                        conn.close(quinn::VarInt::from_u32(0), b"closed by peer");
                        break;
                    }
                    Some(Ok(_)) => {
                        last_activity = tokio::time::Instant::now();
                    }
                    Some(Err(_)) | None => {
                        conn.close(quinn::VarInt::from_u32(2), b"control stream closed");
                        break;
                    }
                }
            }
        }
    }
}
