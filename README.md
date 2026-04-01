![banner](/assets/protofish-banner.png)

# protofish2-rs

Rust implementation of the Protofish2 protocol - a QUIC-based reliable data transfer protocol designed for high-performance, low-latency communication. Combines QUIC's efficiency with custom reliability mechanisms and pluggable compression to handle both ordered/reliable and unordered/unreliable data streams.

## Usage

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
protofish2 = { path = "./path/to/protofish2-rs" }
tokio = { version = "1", features = ["full"] }
```

### Quick Start

#### Server Example

```rust
use protofish2::connection::{ProtofishServer, ServerConfig};
use protofish2::compression::CompressionType;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup TLS certificates and private key
    let cert = load_cert("cert.pem")?;
    let key = load_key("key.pem")?;

    let config = ServerConfig {
        bind_address: "127.0.0.1:5000".parse()?,
        cert_chain: vec![cert],
        private_key: key,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_interval: Duration::from_secs(5),
        protofish_config: Default::default(),
    };

    let server = ProtofishServer::bind(config)?;

    loop {
        if let Some(incoming) = server.accept().await {
            tokio::spawn(async move {
                if let Ok(mut conn) = incoming.accept().await {
                    while let Ok((rel_stream, unrel_stream)) = conn.accept_mani().await {
                        tokio::spawn(async move {
                            // Handle transfer
                            if let Ok(mut recv) = rel_stream.recv().await {
                                while let Some(chunks) = recv.recv().await {
                                    for chunk in chunks {
                                        println!("Received: {:?}", chunk.content);
                                    }
                                }
                            }
                        });
                    }
                }
            });
        }
    }
}
```

#### Client Example

```rust
use protofish2::connection::{ProtofishClient, ClientConfig};
use protofish2::compression::CompressionType;
use protofish2::types::SequenceNumber;
use std::time::Duration;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load root certificate
    let root_cert = load_root_cert("ca.pem")?;

    let config = ClientConfig {
        bind_address: "127.0.0.1:0".parse()?,
        root_certificates: vec![root_cert],
        supported_compression_types: vec![CompressionType::Lz4],
        keepalive_range: Duration::from_secs(1)..Duration::from_secs(10),
        protofish_config: Default::default(),
    };

    let client = ProtofishClient::bind(config)?;
    
    // Connect to server
    let mut conn = client.connect(
        "127.0.0.1:5000".parse()?,
        "server.example.com",
        HashMap::new(),
    ).await?;

    // Open MANI stream
    let mut stream = conn.open_mani().await?;

    // Start transfer with compression
    let mut send_stream = stream.start_transfer(
        CompressionType::Lz4,
        SequenceNumber(0),
        Some(1024), // optional total size
    ).await?;

    // Send data
    let data = b"Hello, Protofish2!";
    send_stream.send(0, data.to_vec()).await?;

    // Signal transfer complete
    stream.end_transfer(SequenceNumber(1)).await?;

    Ok(())
}
```
