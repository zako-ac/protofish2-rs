use dashmap::DashMap;
use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    ManiStreamId, SequenceNumber, Timestamp,
    compression::Compression,
    datagram::packet::{
        FRAG_CONTENT_OVERHEAD, PACKET_WIRE_HEADER_SIZE, Packet, SINGLE_CONTENT_OVERHEAD,
        encode_fragment_content, encode_single_content, serialize_packet,
    },
    mani::{message::TransferMode, transfer::backpressure::BackpressureBank},
};

/// Errors that can occur during a send transfer.
#[derive(Error, Debug, Clone)]
pub enum TransferSendError {
    /// The retransmission buffer has reached its maximum capacity.
    /// Send fewer chunks before receiving acknowledgments.
    #[error("retransmission buffer is full")]
    RetransmissionBufferFull,

    /// Failed to send data via QUIC datagram.
    #[error("failed to send datagram: {0}")]
    DatagramSendFailed(String),

    /// Compression initialization failed.
    #[error("compression failed")]
    CompressionFailed,
}

/// Internal command for transfer state management.
#[doc(hidden)]
pub(crate) enum TransferSendCommand {
    EndTransfer {
        final_sequence_number: SequenceNumber,
        response: oneshot::Sender<Result<(), TransferSendError>>,
    },
    SendTransferEndAck,
}

/// A stream for reliably sending data with compression support.
///
/// This stream handles compression, chunking, and buffering for retransmission.
/// Chunks are automatically retransmitted if NACKed by the receiver.
///
/// # Examples
///
/// ```ignore
/// let mut send = transfer_send_stream;
/// send.send(Timestamp(0), Bytes::from("chunk 1")).await?;
/// send.send(Timestamp(1), Bytes::from("chunk 2")).await?;
/// send.end().await?;
/// ```
pub struct TransferSendStream {
    id: ManiStreamId,
    mode: TransferMode,
    compression: Box<dyn Compression>,
    quic_connection: quinn::Connection,
    initial_sequence_number: SequenceNumber,
    sequence_counter: SequenceNumber,
    /// Each logical `send()` call occupies one sequence number.
    /// For fragmented sends all fragment datagrams share that same sequence number.
    retransmission_buffer: Arc<DashMap<SequenceNumber, Vec<Packet>>>,
    max_retransmission_buffer_size: usize,
    command_sender: Option<mpsc::Sender<TransferSendCommand>>,
    backpressure_bank: BackpressureBank,
    /// Maximum QUIC datagram size in bytes. `None` disables fragmentation.
    max_datagram_size: Option<usize>,
}

impl TransferSendStream {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: ManiStreamId,
        mode: TransferMode,
        compression: Box<dyn Compression>,
        quic_connection: quinn::Connection,
        initial_sequence_number: SequenceNumber,
        max_retransmission_buffer_size: usize,
        command_sender: mpsc::Sender<TransferSendCommand>,
        retransmission_buffer: Arc<DashMap<SequenceNumber, Vec<Packet>>>,
        backpressure_bank: BackpressureBank,
        max_datagram_size: Option<usize>,
    ) -> Self {
        Self {
            id,
            mode,
            compression,
            quic_connection,
            initial_sequence_number,
            sequence_counter: initial_sequence_number,
            retransmission_buffer,
            max_retransmission_buffer_size,
            command_sender: Some(command_sender),
            backpressure_bank,
            max_datagram_size,
        }
    }

    /// Sends a chunk of data.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - When this chunk was created (in milliseconds)
    /// * `content` - The data to send
    ///
    /// # Examples
    ///
    /// ```ignore
    /// send.send(Timestamp(1000), Bytes::from("Hello")).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// The caller should wait for NACKs to be processed before retrying.
    pub async fn send(
        &mut self,
        timestamp: Timestamp,
        content: Bytes,
    ) -> Result<(), TransferSendError> {
        let compressed = Bytes::from(self.compression.compress(&content));
        let seq = self.sequence_counter;

        let max_single_payload = self
            .max_datagram_size
            .map(|m| m.saturating_sub(PACKET_WIRE_HEADER_SIZE + SINGLE_CONTENT_OVERHEAD));

        let max_frag_payload = self
            .max_datagram_size
            .map(|m| m.saturating_sub(PACKET_WIRE_HEADER_SIZE + FRAG_CONTENT_OVERHEAD));

        let needs_fragment = max_single_payload
            .map(|limit| compressed.len() > limit)
            .unwrap_or(false);

        // Build the list of packets for this logical send.
        // All packets share the same sequence number `seq`.
        let packets: Vec<Packet> = if needs_fragment {
            let frag_size = max_frag_payload.unwrap();
            let frags: Vec<&[u8]> = compressed.chunks(frag_size).collect();
            let total_frags = frags.len() as u16;
            frags
                .into_iter()
                .enumerate()
                .map(|(i, frag)| Packet {
                    stream_id: self.id,
                    sequence_number: seq,
                    timestamp,
                    content: encode_fragment_content(i as u16, total_frags, frag),
                })
                .collect()
        } else {
            vec![Packet {
                stream_id: self.id,
                sequence_number: seq,
                timestamp,
                content: encode_single_content(compressed),
            }]
        };

        // Send all datagrams on the wire.
        for packet in &packets {
            self.quic_connection
                .send_datagram(serialize_packet(packet))
                .map_err(|e| TransferSendError::DatagramSendFailed(e.to_string()))?;
        }

        // Store in retransmission buffer (Dual mode only).
        if self.mode == TransferMode::Dual {
            if self.retransmission_buffer.len() >= self.max_retransmission_buffer_size {
                let oldest_seq = SequenceNumber(
                    seq.0
                        .wrapping_sub(self.max_retransmission_buffer_size as u32),
                );
                self.retransmission_buffer.remove(&oldest_seq);
            }
            self.retransmission_buffer.insert(seq, packets.clone());
        }

        // One sequence number per logical send.
        self.sequence_counter = SequenceNumber(seq.0.wrapping_add(1));

        // Backpressure: one credit per datagram sent (credits are per-datagram, not per-send).
        for _ in 0..packets.len() {
            self.backpressure_bank.wait_for_credit().await;
            self.backpressure_bank.decrease_credits(1);
        }

        Ok(())
    }

    /// Gets the current sequence number.
    ///
    /// This is the sequence number that will be used for the next chunk.
    pub fn current_sequence_number(&self) -> SequenceNumber {
        self.sequence_counter
    }

    /// Signals the end of the transfer and waits for acknowledgment.
    ///
    /// After calling this, no more data can be sent. The peer will be notified
    /// and must acknowledge the transfer end.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// send.end().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails or if acknowledgment is not received
    /// within the timeout period (5 seconds).
    pub async fn end(&mut self) -> Result<(), TransferSendError> {
        tracing::debug!("Ending transfer with stream ID {}", self.id.0);

        // If no data was sent, use initial_sequence_number as the final sequence number.
        // This signals to the receiver that the transfer is empty and the ack can be sent immediately.
        let final_sequence_number = if self.sequence_counter != self.initial_sequence_number {
            SequenceNumber(self.sequence_counter.0.wrapping_sub(1))
        } else {
            self.initial_sequence_number
        };

        if let Some(command_sender) = &self.command_sender {
            let (response_tx, response_rx) = oneshot::channel();

            command_sender
                .send(TransferSendCommand::EndTransfer {
                    final_sequence_number,
                    response: response_tx,
                })
                .await
                .map_err(|_| {
                    TransferSendError::DatagramSendFailed(
                        "Failed to send end transfer command".to_string(),
                    )
                })?;

            response_rx.await.map_err(|_| {
                TransferSendError::DatagramSendFailed(
                    "Failed to receive end transfer response".to_string(),
                )
            })?
        } else {
            Err(TransferSendError::DatagramSendFailed(
                "Command sender not available".to_string(),
            ))
        }
    }
}
