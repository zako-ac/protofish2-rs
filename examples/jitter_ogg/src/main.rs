use hound::{WavSpec, WavWriter};
use ogg::PacketReader;
use protofish2::{
    SequenceNumber, Timestamp, TransferMode,
    compression::CompressionType,
    connection::{ClientConfig, ProtofishClient, ProtofishServer, ServerConfig},
    mani::{stream::ManiTransferRecvStreams, transfer::jitter::OpusJitterBuffer},
};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{collections::HashMap, env, fs::File, path::Path, time::Duration};

fn generate_cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let subject_alt_names = vec!["localhost".to_string()];
    let cert = generate_simple_self_signed(subject_alt_names).unwrap();
    let der_cert = cert.cert.der().to_vec();
    let der_key = cert.signing_key.serialize_der();
    (
        vec![CertificateDer::from(der_cert)],
        PrivateKeyDer::Pkcs8(der_key.into()),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <input.ogg> <output.wav>", args[0]);
        return Ok(());
    }
    let input_path = args[1].clone();
    let output_path = args[2].clone();

    println!("Starting local Protofish2 server and client...");

    let (cert_chain, private_key) = generate_cert();
    let server_config = ServerConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        cert_chain: cert_chain.clone(),
        private_key,
        supported_compression_types: vec![CompressionType::None],
        keepalive_interval: Duration::from_secs(5),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };
    let server = ProtofishServer::bind(server_config).expect("Failed to bind server");
    let server_addr = server.local_addr()?;

    let client_config = ClientConfig {
        bind_address: "127.0.0.1:0".parse().unwrap(),
        root_certificates: cert_chain,
        supported_compression_types: vec![CompressionType::None],
        keepalive_range: Duration::from_secs(1)..Duration::from_secs(10),
        protofish_config: protofish2::config::ProtofishConfig::default(),
    };
    let client = ProtofishClient::bind(client_config).expect("Failed to bind client");

    // Spawn the server task that receives and plays audio.
    let server_task = tokio::spawn(async move {
        println!("Server: Waiting for connection...");
        let incoming = server.accept().await.unwrap();
        let mut conn = incoming.accept().await.unwrap();

        println!("Server: Waiting for stream...");
        let mut stream = conn.accept_mani().await.unwrap();

        let transfer = stream.accept_transfer().await.unwrap();
        let unrel_recv = match transfer {
            ManiTransferRecvStreams::Dual {
                unreliable,
                mut reliable,
            } => {
                tokio::spawn(async move {
                    while let Some(_packet) = reliable.recv().await {
                        // pass
                    }
                });

                unreliable
            }
            ManiTransferRecvStreams::UnreliableOnly { unreliable } => unreliable,
        };

        // Assume standard 48kHz, Stereo, 20ms frame
        let mut jitter = OpusJitterBuffer::new(
            unrel_recv,
            48000,
            opus::Channels::Stereo,
            20,  // frame size ms
            100, // playout delay ms
        )
        .expect("Failed to create OpusJitterBuffer");

        let spec = WavSpec {
            channels: 2,
            sample_rate: 48000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer =
            WavWriter::create(Path::new(&output_path), spec).expect("Failed to create WAV writer");

        println!("Server: Receiving audio...");
        let mut received_frames = 0;
        loop {
            println!("Server: Waiting for next PCM frame...");
            match jitter.yield_pcm().await {
                Ok(Some(pcm)) => {
                    for sample in pcm {
                        writer.write_sample(sample).unwrap();
                    }
                    received_frames += 1;
                }
                Ok(None) => break, // EOF
                Err(e) => {
                    eprintln!("Server: Decode error: {}", e);
                    break;
                }
            }
            println!(
                "Server: Received frame {}, total frames: {}",
                received_frames, received_frames
            );
        }

        println!("Server: Finished receiving. Frames: {}", received_frames);
        writer.finalize().unwrap();
    });

    // Client connects and sends
    println!("Client: Connecting...");
    let mut conn = client
        .connect(server_addr, "localhost", HashMap::new())
        .await?;

    println!("Client: Opening stream...");
    let mut stream = conn.open_mani().await?;
    let mut transfer = stream
        .start_transfer(
            TransferMode::Dual,
            CompressionType::None,
            SequenceNumber(0),
            None,
        )
        .await?;

    println!("Client: Reading Ogg file...");
    let file = File::open(&input_path)?;
    let mut reader = PacketReader::new(file);

    let mut timestamp_ms: u64 = 0;
    let mut sent_frames = 0;

    // Fast-forward simulation: just pump packets in with small delay
    while let Some(packet) = reader.read_packet().unwrap_or(None) {
        if packet.data.starts_with(b"OpusHead") || packet.data.starts_with(b"OpusTags") {
            continue;
        }

        let bytes = bytes::Bytes::from(packet.data.clone());
        transfer.send(Timestamp(timestamp_ms), bytes).await?;

        timestamp_ms += 20; // Assume 20ms frames for simplicity
        sent_frames += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    println!(
        "Client: Done sending ({} frames). Ending transfer...",
        sent_frames
    );
    transfer.end().await?;

    server_task.await?;
    println!("Simulation complete.");

    Ok(())
}
