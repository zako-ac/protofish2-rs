use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use protofish2::Timestamp;
use protofish2::compression::CompressionType;
use protofish2::connection::{ClientConfig, ProtofishClient, ProtofishServer, ServerConfig};
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

#[tokio::test]
async fn test_mani_and_transfer() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (cert_chain, private_key) = generate_cert();

    let server_config = ServerConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        cert_chain: cert_chain.clone(),
        private_key,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_interval: Duration::from_secs(5),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let server = ProtofishServer::bind(server_config).expect("Failed to bind server");
    let server_addr = server.local_addr().unwrap();

    let client_config = ClientConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        root_certificates: cert_chain,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_range: Duration::from_secs(1)..Duration::from_secs(10),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let client = ProtofishClient::bind(client_config).expect("Failed to bind client");

    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("No incoming connection");
        let mut server_conn = incoming
            .accept()
            .await
            .expect("Server failed to accept handshake");

        // Server accepts mani stream
        let mut mani_stream = server_conn
            .accept_mani()
            .await
            .expect("Failed to accept mani stream");

        // Read payload
        let payload = mani_stream
            .recv_payload()
            .await
            .expect("Failed to receive payload");
        assert_eq!(payload, Bytes::from("hello from client"));

        // Send payload response
        mani_stream
            .send_payload(Bytes::from("hello from server"))
            .await
            .expect("Failed to send payload");

        // Accept transfer
        let streams = mani_stream
            .accept_transfer()
            .await
            .expect("Failed to accept transfer");

        let (mut reliable_recv, _unreliable_recv) = match streams {
            protofish2::ManiTransferRecvStreams::Dual { reliable, unreliable } => (reliable, unreliable),
            protofish2::ManiTransferRecvStreams::UnreliableOnly { .. } => panic!("Expected dual streams"),
        };

        let mut received_chunks = 0;
        while let Some(chunks) = reliable_recv.recv().await {
            for chunk in chunks {
                assert_eq!(
                    chunk.content,
                    Bytes::from(format!("chunk {}", received_chunks))
                );
                received_chunks += 1;
            }
        }
        assert_eq!(received_chunks, 10);
    });

    let mut client_conn = client
        .connect(server_addr, "localhost", HashMap::new())
        .await
        .expect("Client failed to connect and handshake");

    // Client opens mani stream
    let mut mani_stream = client_conn
        .open_mani()
        .await
        .expect("Failed to open mani stream");

    // Send payload
    mani_stream
        .send_payload(Bytes::from("hello from client"))
        .await
        .expect("Failed to send payload");

    // Recv payload response
    let payload = mani_stream
        .recv_payload()
        .await
        .expect("Failed to receive payload");
    assert_eq!(payload, Bytes::from("hello from server"));

    // Start transfer
    let mut send_stream = mani_stream
        .start_transfer(protofish2::TransferMode::Dual, CompressionType::None, protofish2::SequenceNumber(0), None)
        .await
        .expect("Failed to start transfer");

    // Send 10 chunks
    for i in 0..10 {
        send_stream
            .send(Timestamp(i as u64), Bytes::from(format!("chunk {}", i)))
            .await
            .expect("Failed to send chunk");
    }

    // End transfer
    send_stream.end().await.expect("Failed to end transfer");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_unreliable_only_transfer() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (cert_chain, private_key) = generate_cert();

    let server_config = ServerConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        cert_chain: cert_chain.clone(),
        private_key,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_interval: Duration::from_secs(5),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let server = ProtofishServer::bind(server_config).expect("Failed to bind server");
    let server_addr = server.local_addr().unwrap();

    let client_config = ClientConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        root_certificates: cert_chain,
        supported_compression_types: vec![CompressionType::Lz4, CompressionType::None],
        keepalive_range: Duration::from_secs(1)..Duration::from_secs(10),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };

    let client = ProtofishClient::bind(client_config).expect("Failed to bind client");

    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("No incoming connection");
        let mut server_conn = incoming.accept().await.expect("Server failed to accept");
        let mut mani_stream = server_conn.accept_mani().await.expect("Failed to accept mani stream");

        let streams = mani_stream.accept_transfer().await.expect("Failed to accept transfer");

        let mut unreliable_recv = match streams {
            protofish2::ManiTransferRecvStreams::UnreliableOnly { unreliable } => unreliable,
            protofish2::ManiTransferRecvStreams::Dual { .. } => panic!("Expected unreliable only stream"),
        };

        let mut received_chunks = 0;
        // Unreliable streams only yield 1 chunk at a time via `recv`
        while let Some(chunk) = unreliable_recv.recv().await {
            assert_eq!(
                chunk.content,
                Bytes::from(format!("chunk {}", received_chunks))
            );
            received_chunks += 1;
            if received_chunks == 10 {
                break;
            }
        }
        assert_eq!(received_chunks, 10);
    });

    let mut client_conn = client
        .connect(server_addr, "localhost", HashMap::new())
        .await
        .expect("Client failed to connect and handshake");

    let mut mani_stream = client_conn.open_mani().await.expect("Failed to open mani stream");

    let mut send_stream = mani_stream
        .start_transfer(protofish2::TransferMode::UnreliableOnly, CompressionType::None, protofish2::SequenceNumber(0), None)
        .await
        .expect("Failed to start transfer");

    for i in 0..10 {
        send_stream
            .send(Timestamp(i as u64), Bytes::from(format!("chunk {}", i)))
            .await
            .expect("Failed to send chunk");
    }

    send_stream.end().await.expect("Failed to end transfer");
    server_task.await.unwrap();
}
