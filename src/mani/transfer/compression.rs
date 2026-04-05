use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    compression::Compression, datagram::packet::Packet, mani::transfer::recv::RecvSenderCommand,
};

pub struct CompressedPacketReceiver {
    receiver: mpsc::Receiver<Packet>,
    senders: Vec<mpsc::Sender<Packet>>,
    compression: Box<dyn Compression>,
    sender_command_sender: mpsc::Sender<RecvSenderCommand>,
    credits_bulk_update_count: usize,
    updated_credits: usize,
}

impl CompressedPacketReceiver {
    pub fn new(
        receiver: mpsc::Receiver<Packet>,
        senders: Vec<mpsc::Sender<Packet>>,
        compression: Box<dyn Compression>,
        sender_command_sender: mpsc::Sender<RecvSenderCommand>,
        credits_bulk_update_count: usize,
    ) -> Self {
        Self {
            receiver,
            senders,
            compression,
            sender_command_sender,
            credits_bulk_update_count,
            updated_credits: 0,
        }
    }

    pub async fn run(&mut self) {
        while let Some(packet) = self.receiver.recv().await {
            let compressed_packet = self.compression.decompress(&packet.content);
            let mut any_success = false;

            for sender in &self.senders {
                if sender
                    .send(Packet {
                        stream_id: packet.stream_id,
                        sequence_number: packet.sequence_number,
                        timestamp: packet.timestamp,
                        content: Bytes::from(compressed_packet.clone()),
                    })
                    .await
                    .is_ok()
                {
                    any_success = true;
                } else {
                    tracing::debug!("Failed to send compressed packet, receiver likely dropped");
                }
            }

            if !any_success && !self.senders.is_empty() {
                tracing::debug!(
                    "All receivers dropped for stream {}, stopping CompressedPacketReceiver",
                    packet.stream_id.0
                );
                break;
            }

            self.updated_credits += 1;
            if self.updated_credits >= self.credits_bulk_update_count {
                let command = RecvSenderCommand::UpdateCredits {
                    additional_credits: self.updated_credits,
                };
                if let Err(err) = self.sender_command_sender.send(command).await {
                    tracing::trace!("Failed to send credits update: {}", err);
                }
                self.updated_credits = 0;
            }
        }
    }
}
