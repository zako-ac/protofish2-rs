use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use quinn::ConnectionError;
use tokio::sync::mpsc::{Sender, error::TrySendError};
use tokio::time::Instant;

use crate::{
    ManiStreamId,
    datagram::packet::{Packet, parse_packet},
};

#[derive(Clone)]
pub struct DatagramPacketRouter {
    senders: Arc<DashMap<ManiStreamId, Sender<Packet>>>,
    pending_packets: Arc<DashMap<ManiStreamId, Vec<(Instant, Packet)>>>,
    pub max_packet_buffer_size: usize,
    pub pending_packet_timeout: Duration,
    pub pending_packet_cleanup_interval: Duration,
}

impl DatagramPacketRouter {
    pub fn new(
        max_packet_buffer_size: usize,
        pending_packet_timeout: Duration,
        pending_packet_cleanup_interval: Duration,
    ) -> Self {
        Self {
            senders: Arc::new(DashMap::new()),
            pending_packets: Arc::new(DashMap::new()),
            max_packet_buffer_size,
            pending_packet_timeout,
            pending_packet_cleanup_interval,
        }
    }

    pub fn register(&mut self, stream_id: ManiStreamId, sender: Sender<Packet>) {
        self.senders.insert(stream_id, sender.clone());

        // Flush any pending packets for this stream
        if let Some((_, packets)) = self.pending_packets.remove(&stream_id) {
            tracing::trace!(
                "Flushing {} pending packets for stream {}",
                packets.len(),
                stream_id
            );
            tokio::spawn(async move {
                for (_, packet) in packets {
                    if sender.send(packet).await.is_err() {
                        tracing::debug!(
                            "Failed to send pending packet to stream {}, stopping flush",
                            stream_id
                        );
                        break;
                    }
                }
            });
        }
    }

    pub async fn run(&self, quic: quinn::Connection) {
        let mut cleanup_interval = tokio::time::interval(self.pending_packet_cleanup_interval);

        loop {
            tokio::select! {
                _ = cleanup_interval.tick() => {
                    self.cleanup_pending_packets();
                }
                datagram_res = quic.read_datagram() => {
                    match datagram_res {
                        Ok(datagram) => {
                            tracing::trace!("Received datagram with length {}", datagram.len());

                            if let Ok(packet) = parse_packet(datagram) {
                                self.route(packet.stream_id, packet);
                            } else {
                                tracing::warn!("Failed to parse datagram into packet");
                            }
                        }
                        Err(ConnectionError::ApplicationClosed(_)) => {
                            tracing::debug!("Connection closed by peer");
                            break;
                        }
                        Err(err) => {
                            tracing::warn!("Failed to read datagram: {}", err);
                            break;
                        }
                    }
                }
            }
        }
    }

    fn route(&self, stream_id: ManiStreamId, packet: Packet) {
        let mut remove_senders = Vec::new();
        if let Some(sender) = self.senders.get(&stream_id) {
            let sequence_number = packet.sequence_number;
            match sender.try_send(packet) {
                Ok(_) => {
                    tracing::trace!(
                        "Routed packet with sequence number {} to stream {}",
                        sequence_number,
                        stream_id
                    );
                }
                Err(TrySendError::Full(_)) => {
                    tracing::error!(
                        "Tried to send packet with sequence number {} to stream {}, but the channel is full. Dropping packet",
                        sequence_number,
                        stream_id
                    );

                    // backpressure
                }
                Err(TrySendError::Closed(_)) => {
                    remove_senders.push(stream_id);
                }
            }
        } else {
            tracing::trace!(
                "Received packet for unregistered stream {} with sequence number {}, buffering it",
                stream_id,
                packet.sequence_number
            );
            let mut entry = self
                .pending_packets
                .entry(stream_id)
                .or_insert_with(Vec::new);
            if entry.len() < self.max_packet_buffer_size {
                entry.push((Instant::now(), packet));
            } else {
                tracing::warn!(
                    "Pending packet buffer full for stream {}, dropping sequence number {}",
                    stream_id,
                    packet.sequence_number
                );
            }
        }

        for stream_id in remove_senders {
            self.senders.remove(&stream_id);
        }
    }

    fn cleanup_pending_packets(&self) {
        let now = Instant::now();
        self.pending_packets.retain(|_, packets| {
            packets.retain(|(timestamp, _)| {
                now.duration_since(*timestamp) < self.pending_packet_timeout
            });
            !packets.is_empty()
        });
    }
}
