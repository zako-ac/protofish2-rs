use dashmap::DashMap;
use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    ManiStreamId, SequenceNumber, Timestamp,
    compression::Compression,
    datagram::packet::{Packet, serialize_packet},
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
    sequence_counter: SequenceNumber,
    retransmission_buffer: Arc<DashMap<SequenceNumber, Packet>>,
    max_retransmission_buffer_size: usize,
    command_sender: Option<mpsc::Sender<TransferSendCommand>>,
    backpressure_bank: BackpressureBank,
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
        retransmission_buffer: Arc<DashMap<SequenceNumber, Packet>>,
        backpressure_bank: BackpressureBank,
    ) -> Self {
        Self {
            id,
            mode,
            compression,
            quic_connection,
            sequence_counter: initial_sequence_number,
            retransmission_buffer,
            max_retransmission_buffer_size,
            command_sender: Some(command_sender),
            backpressure_bank,
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
        let compressed_content = self.compression.compress(&content);

        let packet = Packet {
            stream_id: self.id,
            sequence_number: self.sequence_counter,
            timestamp,
            content: Bytes::from(compressed_content),
        };

        let serialized = serialize_packet(&packet);

        self.quic_connection
            .send_datagram(serialized)
            .map_err(|e| TransferSendError::DatagramSendFailed(e.to_string()))?;

        if self.mode == TransferMode::Dual {
            if self.retransmission_buffer.len() >= self.max_retransmission_buffer_size {
                let oldest_seq = SequenceNumber(
                    self.sequence_counter
                        .0
                        .wrapping_sub(self.max_retransmission_buffer_size as u32),
                );
                self.retransmission_buffer.remove(&oldest_seq);
            }
            self.retransmission_buffer
                .insert(self.sequence_counter, packet);
        }

        self.sequence_counter = SequenceNumber(self.sequence_counter.0.wrapping_add(1));

        self.backpressure_bank.wait_for_credit().await;
        self.backpressure_bank.decrease_credits(1);

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
        let final_sequence_number = SequenceNumber(self.sequence_counter.0.wrapping_sub(1));

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
