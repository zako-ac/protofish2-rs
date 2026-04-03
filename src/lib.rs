//! # Protofish2 - QUIC-based Reliable Data Transfer Protocol
//!
//! Protofish2 is a high-performance protocol built on top of QUIC (Quinn) that provides
//! reliable data transfer with compression support. It enables clients and servers to establish
//! connections and transfer data with configurable retransmission and compression capabilities.
//!
//! ## Key Features
//!
//! - **Reliable data transfer** over QUIC with automatic retransmission
//! - **Optional compression** (configurable at transfer time)
//! - **Async/await support** with Tokio runtime
//! - **Stream-based API** for sending and receiving data
//! - **Efficient datagram routing** for reliable chunk delivery
//!
//! ## Quick Start
//!
//! ### Creating a Server
//!
//! ```ignore
//! use protofish2::connection::{ProtofishServer, ServerConfig};
//! use std::net::SocketAddr;
//!
//! let config = ServerConfig {
//!     bind_address: "127.0.0.1:5000".parse().unwrap(),
//!     cert_chain: vec![/* your certificates */],
//!     private_key: /* your private key */,
//!     supported_compression_types: vec![CompressionType::None],
//!     keepalive_interval: Duration::from_secs(30),
//!     protofish_config: ProtofishConfig::default(),
//! };
//!
//! let server = ProtofishServer::bind(config)?;
//!
//! loop {
//!     let incoming = server.accept().await.unwrap();
//!     let conn = incoming.accept().await?;
//!     // Handle connection...
//! }
//! ```
//!
//! ### Creating a Client
//!
//! ```ignore
//! use protofish2::connection::{ProtofishClient, ClientConfig};
//! use std::net::SocketAddr;
//!
//! let config = ClientConfig {
//!     bind_address: "127.0.0.1:0".parse().unwrap(),
//!     root_certificates: vec![/* your certificates */],
//!     supported_compression_types: vec![CompressionType::None],
//!     keepalive_range: Duration::from_secs(10)..Duration::from_secs(30),
//!     protofish_config: ProtofishConfig::default(),
//! };
//!
//! let client = ProtofishClient::bind(config)?;
//! let mut conn = client.connect(
//!     "127.0.0.1:5000".parse().unwrap(),
//!     "example.com",
//!     HashMap::new(),
//! ).await?;
//! ```
//!
//! ### Transferring Data
//!
//! ```ignore
//! let mut send_stream = conn.open_mani().await?;
//! let mut reliable_send = send_stream.start_transfer(
//!     protofish2::TransferMode::Dual,
//!     CompressionType::None,
//!     protofish2::SequenceNumber(0),
//!     None,
//! ).await?;
//!
//! reliable_send.send(Timestamp(0), Bytes::from("Hello, World!")).await?;
//! reliable_send.end().await?;
//! ```

mod codec;
pub mod compression;
pub mod connection;
pub mod mani;

pub mod config;
pub mod error;

pub mod types;
pub use types::*;
pub use mani::message::TransferMode;
pub use mani::stream::ManiTransferRecvStreams;

mod datagram;
