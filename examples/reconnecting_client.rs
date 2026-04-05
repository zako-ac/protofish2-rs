use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use protofish2::compression::CompressionType;
use protofish2::config::ReconnectConfig;
use protofish2::connection::{
    ClientConfig, ProtofishClient, ProtofishServer, ReconnectingConnection, ServerConfig,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn generate_cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let subject_alt_names = vec!["localhost".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();
    let der_cert = cert.cert.der().to_vec();
    let der_key = cert.signing_key.serialize_der();
    (
        vec![CertificateDer::from(der_cert)],
        PrivateKeyDer::Pkcs8(der_key.into()),
    )
}

async fn start_server(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    port: u16,
) -> std::net::SocketAddr {
    let server_config = ServerConfig {
        bind_address: format!("127.0.0.1:{}", port).parse().unwrap(),
        cert_chain,
        private_key,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_interval: Duration::from_secs(5),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let server = ProtofishServer::bind(server_config).expect("Failed to bind server");
    let server_addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        tracing::info!("Server listening on {}...", server_addr);
        while let Some(incoming) = server.accept().await {
            tracing::info!("Server received an incoming connection request...");
            if let Ok(conn) = incoming.accept().await {
                tracing::info!("Server successfully handshaked with client!");
                // Keep the connection alive briefly then simulate a crash by dropping it
                tokio::time::sleep(Duration::from_millis(500)).await;
                tracing::warn!("Server intentionally dropping connection to simulate a crash...");
                drop(conn);
                server.close(0, b"Simulated crash");
                break; // Simulates server crash/restart cycle
            }
        }
    });

    server_addr
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Initialize tracing for logs
    tracing_subscriber::fmt::init();
    tracing::info!("Starting reconnecting_client example...");

    // Generate self-signed certificate for the example
    let (cert_chain, private_key) = generate_cert();

    // 1. Setup the Server (and simulate crash/restart loop)
    let server_addr = start_server(cert_chain.clone(), private_key.clone_key(), 0).await;

    // 2. Setup the Client
    let client_config = ClientConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        root_certificates: cert_chain.clone(),
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_range: Duration::from_secs(1)..Duration::from_secs(10),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let client = Arc::new(ProtofishClient::bind(client_config).expect("Failed to bind client"));
    tracing::info!("Client bound locally");

    // 3. Connect using ReconnectingConnection wrapper
    let reconnect_config = ReconnectConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(2),
        backoff_multiplier: 1.5,
        max_retries: Some(10),
    };

    tracing::info!(
        "Client attempting resilient connection to {}...",
        server_addr
    );
    let mut conn = ReconnectingConnection::connect(
        client,
        server_addr,
        "localhost".to_string(),
        HashMap::new(),
        reconnect_config,
    )
    .await
    .expect("Failed initial resilient connection");

    tracing::info!("Client connected successfully!");

    // 4. Try opening streams in a loop. When the server drops the connection,
    // open_mani will block, reconnect internally, and resume execution.
    for i in 0..3 {
        tracing::info!("Client attempting to open stream {}...", i);
        match conn.open_mani().await {
            Ok(_stream) => {
                tracing::info!("Stream {} opened successfully!", i);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                tracing::error!("Stream failed irrecoverably: {}", e);
            }
        }

        // Restart server after the first crash to allow reconnects
        if i == 0 {
            tokio::time::sleep(Duration::from_millis(800)).await;
            tracing::info!("--- Restarting server! ---");
            let _ = start_server(
                cert_chain.clone(),
                private_key.clone_key(),
                server_addr.port(),
            )
            .await;
        }
    }

    tracing::info!("Example completed successfully!");
}
