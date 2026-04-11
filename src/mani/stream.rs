use dashmap::DashMap;
use std::sync::{Arc, atomic::AtomicBool};
use thiserror::Error;
use tokio::sync::{Notify, mpsc::Sender};

use bytes::Bytes;
use tokio::sync::{mpsc::Receiver, oneshot};

use crate::{
    ManiStreamId, SequenceNumber,
    compression::CompressionType,
    datagram::{packet::Packet, router::DatagramPacketRouter},
    mani::{
        frame::{ManiReadFrame, ManiWriteFrame},
        message::{
            ManiMessage, ManiNack, TransferCreditsUpdate, TransferError, TransferErrorCode,
            TransferMode,
        },
        transfer::{
            backpressure::BackpressureBank,
            compression::CompressedPacketReceiver,
            recv::{
                RecvPipelineCommand, RecvSenderCommand, TransferReliableRecvStream,
                TransferUnreliableRecvStream, create_stream_pair,
            },
            send::{TransferSendCommand, TransferSendError, TransferSendStream},
        },
    },
};

enum ManiStreamRole {
    Unknown,
    Sender {
        retransmission_buffer: Arc<DashMap<SequenceNumber, Vec<Packet>>>,
        transfer_end_ack_sender: Option<oneshot::Sender<()>>,
    },
    Receiver {
        retrans_packet_sender: Sender<Packet>,
        pipeline_command_sender: Option<Sender<RecvPipelineCommand>>,
        initial_sequence_number: Option<SequenceNumber>,
    },
}

enum ManiStreamCommand {
    StartTransfer {
        mode: TransferMode,
        compression: CompressionType,
        initial_sequence_number: SequenceNumber,
        data_size: Option<u64>,
        response: oneshot::Sender<Result<TransferSendStream, TransferSendError>>,
    },
    EndTransfer {
        final_sequence_number: SequenceNumber,
        response: oneshot::Sender<Result<(), TransferSendError>>,
    },
    SendPayload {
        payload: Bytes,
        response: oneshot::Sender<Result<(), ManiStreamError>>,
    },
}

/// Errors that can occur during Mani stream operations.
#[derive(Error, Debug, Clone)]
pub enum ManiStreamError {
    /// The stream has been closed by the peer.
    #[error("stream closed")]
    StreamClosed,

    /// Expected payload data but received a transfer initiation.
    #[error("expected payload, got transfer")]
    ExpectedPayload,

    /// Expected a transfer initiation but received payload data.
    #[error("expected transfer, got payload")]
    ExpectedTransfer,
}

/// Received transfer streams
pub enum ManiTransferRecvStreams {
    Dual {
        reliable: TransferReliableRecvStream,
        unreliable: TransferUnreliableRecvStream,
    },
    UnreliableOnly {
        unreliable: TransferUnreliableRecvStream,
    },
}

/// Messages that can be received from a stream.
pub enum ManiRecvMessage {
    /// An incoming transfer request with reliable and unreliable receivers.
    Transfer(ManiTransferRecvStreams),

    /// Raw payload data.
    Payload(Bytes),
}

pub(crate) struct ManiStreamTask {
    pub id: ManiStreamId,
    pub max_retransmission_buffer_size: usize,
    pub max_datagram_channel_size: usize,
    pub max_chunk_buffer_size: usize,
    pub credits_bulk_update_count: usize,
    pub max_datagram_size: Option<usize>,

    pub writer: ManiWriteFrame<quinn::SendStream>,
    pub reader: ManiReadFrame<quinn::RecvStream>,

    pub message_sender: Sender<ManiRecvMessage>,
    pub datagram_router: DatagramPacketRouter,

    pub quic_connection: quinn::Connection,

    role: ManiStreamRole,
    command_receiver: Receiver<ManiStreamCommand>,
    transfer_send_command_receiver: Receiver<TransferSendCommand>,
    transfer_send_command_sender: Sender<TransferSendCommand>,
    sender_command_sender: Sender<RecvSenderCommand>,
    sender_command_receiver: Receiver<RecvSenderCommand>,
    end_notify: Arc<Notify>,
    is_ended: Arc<AtomicBool>,

    backpressure_bank: BackpressureBank,
}

impl ManiStreamTask {
    pub async fn run(mut self) {
        self.role = ManiStreamRole::Unknown;

        loop {
            tokio::select! {
                Some(command) = self.command_receiver.recv() => {
                    if !self.handle_command(command).await {
                        break;
                    }
                }
                Some(send_command) = self.transfer_send_command_receiver.recv() => {
                    if !self.handle_transfer_send_command(send_command).await {
                        break;
                    }
                }
                message = self.reader.read_frame() => {
                    if !self.handle_message(message).await {
                        break;
                    }
                }
                Some(cmd) = self.sender_command_receiver.recv() => {
                    if !self.handle_sender_command(cmd).await {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_sender_command(&mut self, command: RecvSenderCommand) -> bool {
        match command {
            RecvSenderCommand::UpdateCredits { additional_credits } => {
                let update_credits_message =
                    ManiMessage::TransferCreditsUpdate(TransferCreditsUpdate {
                        additional_credits,
                    });

                if let Err(err) = self.writer.write_frame(&update_credits_message).await {
                    tracing::debug!(
                        "Failed to send TransferCreditsUpdate on stream {}: {}",
                        self.id,
                        err
                    );
                    return false;
                }

                true
            }
            RecvSenderCommand::Nack { sequence_numbers } => {
                let nack_message = ManiMessage::Nack(ManiNack { sequence_numbers });
                if let Err(err) = self.writer.write_frame(&nack_message).await {
                    tracing::debug!("Failed to send NACK on stream {}: {}", self.id, err);
                    return false;
                }
                true
            }
        }
    }

    async fn handle_transfer_send_command(&mut self, command: TransferSendCommand) -> bool {
        match command {
            TransferSendCommand::EndTransfer {
                final_sequence_number,
                response,
            } => {
                match self.end_transfer_internal(final_sequence_number).await {
                    Ok(ack_rx) => {
                        tokio::spawn(async move {
                            match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx)
                                .await
                            {
                                Ok(Ok(())) => {
                                    let _ = response.send(Ok(()));
                                }
                                Ok(Err(_)) => {
                                    let _ =
                                        response.send(Err(TransferSendError::DatagramSendFailed(
                                            "TransferEndAck channel closed".to_string(),
                                        )));
                                }
                                Err(_) => {
                                    let _ =
                                        response.send(Err(TransferSendError::DatagramSendFailed(
                                            "Timeout waiting for TransferEndAck".to_string(),
                                        )));
                                }
                            }
                        });
                    }
                    Err(err) => {
                        let _ = response.send(Err(err));
                    }
                }
                true
            }
            TransferSendCommand::SendTransferEndAck => {
                self.end();
                if let Err(err) = self.writer.write_frame(&ManiMessage::TransferEndAck).await {
                    tracing::debug!(
                        "Failed to send TransferEndAck on stream {}: {}",
                        self.id,
                        err
                    );
                    return false;
                }
                true
            }
        }
    }

    fn end(&mut self) {
        self.is_ended
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.end_notify.notify_waiters();
    }

    async fn handle_command(&mut self, command: ManiStreamCommand) -> bool {
        match command {
            ManiStreamCommand::StartTransfer {
                mode,
                compression,
                initial_sequence_number,
                data_size,
                response,
            } => {
                let result = self
                    .start_transfer_internal(mode, compression, initial_sequence_number, data_size)
                    .await;
                let _ = response.send(result);
                true
            }
            ManiStreamCommand::EndTransfer {
                final_sequence_number,
                response,
            } => {
                match self.end_transfer_internal(final_sequence_number).await {
                    Ok(ack_rx) => {
                        tokio::spawn(async move {
                            match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx)
                                .await
                            {
                                Ok(Ok(())) => {
                                    let _ = response.send(Ok(()));
                                }
                                Ok(Err(_)) => {
                                    let _ =
                                        response.send(Err(TransferSendError::DatagramSendFailed(
                                            "TransferEndAck channel closed".to_string(),
                                        )));
                                }
                                Err(_) => {
                                    let _ =
                                        response.send(Err(TransferSendError::DatagramSendFailed(
                                            "Timeout waiting for TransferEndAck".to_string(),
                                        )));
                                }
                            }
                        });
                    }
                    Err(err) => {
                        let _ = response.send(Err(err));
                    }
                }
                true
            }
            ManiStreamCommand::SendPayload { payload, response } => {
                let result = self
                    .writer
                    .write_frame(&crate::mani::message::ManiMessage::Payload(
                        crate::mani::message::ManiPayload { payload },
                    ))
                    .await;
                let _ = response.send(result.map_err(|_| ManiStreamError::StreamClosed));
                true
            }
        }
    }

    async fn end_transfer_internal(
        &mut self,
        final_sequence_number: SequenceNumber,
    ) -> Result<oneshot::Receiver<()>, TransferSendError> {
        let (ack_tx, ack_rx) = oneshot::channel();

        match &mut self.role {
            ManiStreamRole::Sender {
                transfer_end_ack_sender,
                ..
            } => {
                *transfer_end_ack_sender = Some(ack_tx);
            }
            _ => {
                return Err(TransferSendError::DatagramSendFailed(
                    "Not in Sender role".to_string(),
                ));
            }
        }

        if let Err(err) = self
            .writer
            .write_frame(&ManiMessage::TransferEnd(
                crate::mani::message::TransferEnd {
                    final_sequence_number,
                },
            ))
            .await
        {
            return Err(TransferSendError::DatagramSendFailed(err.to_string()));
        }

        // After signaling the end of the transfer, we can send a credits update to help flush any
        // remaining data through the pipeline.
        let credits_update_message = ManiMessage::TransferCreditsUpdate(TransferCreditsUpdate {
            additional_credits: self.credits_bulk_update_count,
        });
        if let Err(err) = self.writer.write_frame(&credits_update_message).await {
            tracing::debug!(
                "Failed to send TransferCreditsUpdate on stream {}: {}",
                self.id,
                err
            );
            return Err(TransferSendError::DatagramSendFailed(err.to_string()));
        }

        Ok(ack_rx)
    }

    async fn start_transfer_internal(
        &mut self,
        mode: TransferMode,
        compression: CompressionType,
        initial_sequence_number: SequenceNumber,
        data_size: Option<u64>,
    ) -> Result<TransferSendStream, TransferSendError> {
        let compression_impl = compression
            .to_boxed_compression()
            .ok_or(TransferSendError::CompressionFailed)?;

        if let Err(err) = self
            .writer
            .write_frame(&ManiMessage::TransferStart(
                crate::mani::message::TransferStart {
                    mode,
                    compression_type: compression,
                    initial_sequence_number,
                    data_size,
                },
            ))
            .await
        {
            return Err(TransferSendError::DatagramSendFailed(err.to_string()));
        }

        let retransmission_buffer: Arc<DashMap<SequenceNumber, Vec<Packet>>> =
            Arc::new(DashMap::new());

        self.role = ManiStreamRole::Sender {
            retransmission_buffer: Arc::clone(&retransmission_buffer),
            transfer_end_ack_sender: None,
        };

        let effective_max_datagram_size = self
            .max_datagram_size
            .or_else(|| self.quic_connection.max_datagram_size());

        Ok(TransferSendStream::new(
            self.id,
            mode,
            compression_impl,
            self.quic_connection.clone(),
            initial_sequence_number,
            self.max_retransmission_buffer_size,
            self.transfer_send_command_sender.clone(),
            retransmission_buffer,
            self.backpressure_bank.clone(),
            effective_max_datagram_size,
        ))
    }

    async fn handle_message(&mut self, message: Option<ManiMessage>) -> bool {
        if let Some(message) = message {
            tracing::trace!("Received message on stream {}: {:?}", self.id, message);

            match message {
                ManiMessage::TransferStart(transfer_start) => {
                    self.handle_transfer_start(transfer_start).await
                }
                ManiMessage::Payload(payload) => self.handle_payload(payload).await,
                ManiMessage::Nack(nack) => self.handle_nack(nack).await,
                ManiMessage::Retrans(retrans) => self.handle_retrans(retrans).await,
                ManiMessage::TransferEnd(transfer_end) => {
                    self.handle_transfer_end(transfer_end).await
                }
                ManiMessage::TransferEndAck => self.handle_transfer_end_ack().await,
                ManiMessage::TransferError(transfer_error) => {
                    self.handle_transfer_error(transfer_error).await
                }
                ManiMessage::TransferCreditsUpdate(update) => {
                    self.backpressure_bank
                        .increase_credits(update.additional_credits);

                    true
                }
                _ => {
                    tracing::debug!(
                        "Received unsupported message type on stream {}: {:?}",
                        self.id,
                        message
                    );
                    true
                }
            }
        } else {
            tracing::debug!("Stream {} closed by peer", self.id);
            false
        }
    }

    async fn handle_transfer_start(
        &mut self,
        transfer_start: crate::mani::message::TransferStart,
    ) -> bool {
        if let Some(compression) = transfer_start.compression_type.to_boxed_compression() {
            let (dg_sender, dg_receiver) =
                tokio::sync::mpsc::channel(self.max_datagram_channel_size);

            self.datagram_router.register(self.id, dg_sender.clone());

            let transfer_streams = if transfer_start.mode == TransferMode::Dual {
                let (s1, r1) = tokio::sync::mpsc::channel(self.max_datagram_channel_size);
                let (s2, r2) = tokio::sync::mpsc::channel(self.max_datagram_channel_size);
                let (pipeline_tx, pipeline_rx) = tokio::sync::mpsc::channel(1);

                let mut compression_receiver = CompressedPacketReceiver::new(
                    dg_receiver,
                    vec![s1, s2],
                    compression,
                    self.sender_command_sender.clone(),
                    self.credits_bulk_update_count,
                    self.max_chunk_buffer_size,
                );

                tokio::spawn(async move {
                    compression_receiver.run().await;
                });

                let (rel, unrel) = create_stream_pair(
                    self.id,
                    r1,
                    r2,
                    self.end_notify.clone(),
                    self.is_ended.clone(),
                    self.sender_command_sender.clone(),
                    self.max_retransmission_buffer_size,
                    pipeline_rx,
                )
                .await;

                self.role = ManiStreamRole::Receiver {
                    retrans_packet_sender: dg_sender,
                    pipeline_command_sender: Some(pipeline_tx),
                    initial_sequence_number: Some(transfer_start.initial_sequence_number),
                };

                ManiTransferRecvStreams::Dual {
                    reliable: rel,
                    unreliable: unrel,
                }
            } else {
                let (s1, r1) = tokio::sync::mpsc::channel(self.max_datagram_channel_size);

                let mut compression_receiver = CompressedPacketReceiver::new(
                    dg_receiver,
                    vec![s1],
                    compression,
                    self.sender_command_sender.clone(),
                    self.credits_bulk_update_count,
                    self.max_chunk_buffer_size,
                );

                tokio::spawn(async move {
                    compression_receiver.run().await;
                });

                let unrel = TransferUnreliableRecvStream::new(
                    self.id,
                    self.is_ended.clone(),
                    r1,
                    self.end_notify.clone(),
                )
                .await;

                self.role = ManiStreamRole::Receiver {
                    retrans_packet_sender: dg_sender,
                    pipeline_command_sender: None,
                    initial_sequence_number: Some(transfer_start.initial_sequence_number),
                };

                ManiTransferRecvStreams::UnreliableOnly { unreliable: unrel }
            };

            if let Err(err) = self
                .message_sender
                .send(ManiRecvMessage::Transfer(transfer_streams))
                .await
            {
                tracing::debug!("Failed to send transfer streams to channel: {}", err);
                return false;
            }
            true
        } else {
            tracing::debug!(
                "Received unsupported compression type on stream {}: {:?}",
                self.id,
                transfer_start.compression_type
            );

            self.writer
                .write_frame(&ManiMessage::TransferError(TransferError {
                    error_code: TransferErrorCode::UnsupportedCompression,
                }))
                .await
                .unwrap_or_else(|err| {
                    tracing::debug!(
                        "Failed to send TransferError on stream {}: {}",
                        self.id,
                        err
                    );
                });
            true
        }
    }

    async fn handle_payload(&mut self, payload: crate::mani::message::ManiPayload) -> bool {
        if let Err(err) = self
            .message_sender
            .send(ManiRecvMessage::Payload(payload.payload))
            .await
        {
            tracing::debug!("Failed to send payload to channel: {}", err);
            return false;
        }
        true
    }

    async fn handle_nack(&mut self, nack: ManiNack) -> bool {
        // Build one ManiRetrans per datagram (fragment) stored for each NACKed sequence number.
        let retrans_messages: Vec<_> = match &self.role {
            ManiStreamRole::Sender {
                retransmission_buffer,
                ..
            } => {
                let mut msgs = Vec::new();
                for seq_num in &nack.sequence_numbers {
                    if let Some(packets) = retransmission_buffer.get(seq_num) {
                        for packet in packets.value().iter() {
                            msgs.push(crate::mani::message::ManiRetrans {
                                sequence_number: *seq_num,
                                timestamp: packet.timestamp,
                                payload: packet.content.clone(),
                            });
                        }
                    } else {
                        tracing::warn!(
                            "Cannot retransmit seq {} on stream {} - not in buffer",
                            seq_num,
                            self.id
                        );
                    }
                }
                msgs
            }
            _ => {
                tracing::debug!("Received NACK on stream {} but not in Sender role", self.id);
                return true;
            }
        };

        for retrans in retrans_messages {
            if let Err(err) = self
                .writer
                .write_frame(&ManiMessage::Retrans(retrans))
                .await
            {
                tracing::debug!("Failed to send Retrans on stream {}: {}", self.id, err);
                return false;
            }
        }

        true
    }

    async fn handle_retrans(&mut self, retrans: crate::mani::message::ManiRetrans) -> bool {
        match &self.role {
            ManiStreamRole::Receiver {
                retrans_packet_sender,
                ..
            } => {
                let packet = Packet {
                    stream_id: self.id,
                    sequence_number: retrans.sequence_number,
                    timestamp: retrans.timestamp,
                    content: retrans.payload,
                };

                if let Err(err) = retrans_packet_sender.send(packet).await {
                    tracing::debug!(
                        "Failed to send retransmitted chunk to pipeline on stream {}: {}",
                        self.id,
                        err
                    );
                    return false;
                }
                true
            }
            _ => {
                tracing::debug!(
                    "Received Retrans on stream {} but not in Receiver role",
                    self.id
                );
                true
            }
        }
    }

    async fn handle_transfer_end(
        &mut self,
        transfer_end: crate::mani::message::TransferEnd,
    ) -> bool {
        match &self.role {
            ManiStreamRole::Receiver {
                pipeline_command_sender,
                initial_sequence_number,
                ..
            } => {
                tracing::debug!(
                    "Received TransferEnd on stream {} with final seq {}",
                    self.id,
                    transfer_end.final_sequence_number
                );

                // Check if this is an empty transfer: if final_seq == initial_seq,
                // no data was sent, so acknowledge immediately without going through the pipeline.
                let is_empty_transfer = initial_sequence_number
                    .map(|initial| transfer_end.final_sequence_number == initial)
                    .unwrap_or(false);

                if is_empty_transfer {
                    // Empty transfer: send ack immediately
                    tracing::debug!(
                        "Received empty TransferEnd on stream {} (final_seq == initial_seq), acknowledging immediately",
                        self.id
                    );

                    self.end();

                    if let Err(err) = self.writer.write_frame(&ManiMessage::TransferEndAck).await {
                        tracing::debug!(
                            "Failed to send TransferEndAck on stream {}: {}",
                            self.id,
                            err
                        );
                        return false;
                    }
                } else if let Some(pipeline_command_sender) = pipeline_command_sender {
                    let (reply_tx, reply_rx) = oneshot::channel();

                    if let Err(err) = pipeline_command_sender
                        .send(RecvPipelineCommand::EndTransfer {
                            final_sequence_number: transfer_end.final_sequence_number,
                            reply: reply_tx,
                        })
                        .await
                    {
                        tracing::debug!(
                            "Failed to send EndTransfer to pipeline on stream {}: {}",
                            self.id,
                            err
                        );
                        return false;
                    }

                    let command_sender = self.transfer_send_command_sender.clone();
                    tokio::spawn(async move {
                        if reply_rx.await.is_ok() {
                            let _ = command_sender
                                .send(TransferSendCommand::SendTransferEndAck)
                                .await;
                        }
                    });
                } else {
                    // No pipeline to flush, acknowledge immediately

                    self.end();

                    if let Err(err) = self.writer.write_frame(&ManiMessage::TransferEndAck).await {
                        tracing::debug!(
                            "Failed to send TransferEndAck on stream {}: {}",
                            self.id,
                            err
                        );
                        return false;
                    }
                }

                true
            }
            _ => {
                tracing::debug!(
                    "Received TransferEnd on stream {} but not in Receiver role",
                    self.id
                );
                true
            }
        }
    }

    async fn handle_transfer_end_ack(&mut self) -> bool {
        match &mut self.role {
            ManiStreamRole::Sender {
                transfer_end_ack_sender,
                ..
            } => {
                if let Some(sender) = transfer_end_ack_sender.take() {
                    let _ = sender.send(());
                    tracing::debug!("Sent TransferEndAck notification on stream {}", self.id);
                } else {
                    tracing::warn!(
                        "Received TransferEndAck on stream {} but no pending end transfer",
                        self.id
                    );
                }
                true
            }
            _ => {
                tracing::debug!(
                    "Received TransferEndAck on stream {} but not in Sender role",
                    self.id
                );
                true
            }
        }
    }

    async fn handle_transfer_error(&mut self, transfer_error: TransferError) -> bool {
        tracing::error!(
            "Received TransferError on stream {}: {:?}",
            self.id,
            transfer_error.error_code
        );
        true
    }
}

/// A bidirectional stream for exchanging data with a peer.
///
/// A `ManiStream` can be used in two modes:
/// - **Sender mode**: Call `start_transfer()` to begin sending data reliably
/// - **Receiver mode**: Call `accept_transfer()` to receive incoming data
///
/// # Examples
///
/// Sending data:
///
///
/// ```ignore
/// let mut stream = conn.open_mani().await?;
/// let transfer = stream.start_transfer(CompressionType::None, SequenceNumber(0), None).await?;
/// transfer.send(Timestamp(0), Bytes::from("Hello")).await?;
/// transfer.end().await?;
/// ```
///
/// Receiving data:
///
/// ```ignore
/// let mut stream = conn.accept_mani().await?;
/// let (reliable_recv, unreliable_recv) = stream.accept_transfer().await?;
/// while let Some(data) = reliable_recv.recv().await {
///     println!("Received: {:?}", data);
/// }
/// ```
pub struct ManiStream {
    pub id: ManiStreamId,

    message_receiver: Receiver<ManiRecvMessage>,
    command_sender: Sender<ManiStreamCommand>,
}

impl ManiStream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pair(
        id: ManiStreamId,
        quic_connection: quinn::Connection,
        writer: ManiWriteFrame<quinn::SendStream>,
        reader: ManiReadFrame<quinn::RecvStream>,
        datagram_router: DatagramPacketRouter,
        max_retransmission_buffer_size: usize,
        max_nack_channel_size: usize,
        max_datagram_channel_size: usize,
        max_chunk_buffer_size: usize,
        initial_backpressure_credits: usize,
        credits_bulk_update_count: usize,
        max_datagram_size: Option<usize>,
    ) -> (Self, ManiStreamTask) {
        let (message_sender, message_receiver) = tokio::sync::mpsc::channel(100);
        let (command_sender, command_receiver) = tokio::sync::mpsc::channel(100);
        let (transfer_send_command_sender, transfer_send_command_receiver) =
            tokio::sync::mpsc::channel(100);
        let (sc_sender, sc_receiver) = tokio::sync::mpsc::channel(max_nack_channel_size);

        let stream = Self {
            id,
            message_receiver,
            command_sender,
        };

        let task = ManiStreamTask {
            id,
            max_retransmission_buffer_size,
            max_datagram_channel_size,
            max_chunk_buffer_size,
            credits_bulk_update_count,
            max_datagram_size,
            writer,
            reader,
            message_sender,
            datagram_router,
            quic_connection,
            role: ManiStreamRole::Unknown,
            end_notify: Arc::new(Notify::new()),
            is_ended: Arc::new(AtomicBool::new(false)),
            command_receiver,
            transfer_send_command_receiver,
            transfer_send_command_sender,
            sender_command_sender: sc_sender,
            sender_command_receiver: sc_receiver,
            backpressure_bank: BackpressureBank::new(initial_backpressure_credits),
        };

        (stream, task)
    }

    /// Receives the next message from the stream.
    ///
    /// Returns `None` if the stream has been closed by the peer.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// while let Some(msg) = stream.recv().await {
    ///     match msg {
    ///         ManiRecvMessage::Transfer(rel, unrel) => { /* handle transfer */ },
    ///         ManiRecvMessage::Payload(data) => { /* handle payload */ },
    ///     }
    /// }
    /// ```
    pub async fn recv(&mut self) -> Option<ManiRecvMessage> {
        self.message_receiver.recv().await
    }

    /// Receives the next message, expecting it to be payload data.
    ///
    /// Returns an error if the message is a transfer request or if the stream is closed.
    pub async fn recv_payload(&mut self) -> Result<Bytes, ManiStreamError> {
        match self.recv().await {
            Some(ManiRecvMessage::Payload(bytes)) => Ok(bytes),
            Some(ManiRecvMessage::Transfer(_)) => Err(ManiStreamError::ExpectedPayload),
            None => Err(ManiStreamError::StreamClosed),
        }
    }

    /// Accepts an incoming transfer request.
    ///
    /// Returns reliable and unreliable receivers for the transfer.
    /// The reliable receiver delivers data in order with retransmission.
    /// The unreliable receiver may skip damaged chunks.
    ///
    /// # Errors
    ///
    /// Returns an error if the message is payload data or if the stream is closed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let (reliable, unreliable) = stream.accept_transfer().await?;
    /// ```
    pub async fn accept_transfer(&mut self) -> Result<ManiTransferRecvStreams, ManiStreamError> {
        match self.recv().await {
            Some(ManiRecvMessage::Transfer(streams)) => Ok(streams),
            Some(ManiRecvMessage::Payload(_)) => Err(ManiStreamError::ExpectedTransfer),
            None => Err(ManiStreamError::StreamClosed),
        }
    }

    /// Initiates a reliable data transfer.
    ///
    /// # Arguments
    ///
    /// * `compression` - Compression type to use (e.g., `CompressionType::None`)
    /// * `initial_sequence_number` - Starting sequence number for chunks
    /// * `data_size` - Optional total size hint for the transfer (for optimization)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let transfer = stream.start_transfer(
    ///     CompressionType::None,
    ///     SequenceNumber(0),
    ///     None,
    /// ).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer cannot be initiated (e.g., channel failure).
    pub async fn start_transfer(
        &mut self,
        mode: TransferMode,
        compression: CompressionType,
        initial_sequence_number: SequenceNumber,
        data_size: Option<u64>,
    ) -> Result<TransferSendStream, TransferSendError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_sender
            .send(ManiStreamCommand::StartTransfer {
                mode,
                compression,
                initial_sequence_number,
                data_size,
                response: response_tx,
            })
            .await
            .map_err(|_| {
                TransferSendError::DatagramSendFailed(
                    "Failed to send start transfer command".to_string(),
                )
            })?;

        response_rx.await.map_err(|_| {
            TransferSendError::DatagramSendFailed(
                "Failed to receive start transfer response".to_string(),
            )
        })?
    }

    /// Ends the current transfer and waits for acknowledgment from the peer.
    ///
    /// # Arguments
    ///
    /// * `final_sequence_number` - The sequence number of the last chunk sent
    ///
    /// # Examples
    ///
    /// ```ignore
    /// transfer.end().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails or if the peer doesn't acknowledge within timeout.
    pub async fn end_transfer(
        &mut self,
        final_sequence_number: SequenceNumber,
    ) -> Result<(), TransferSendError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_sender
            .send(ManiStreamCommand::EndTransfer {
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
    }

    /// Sends a raw payload message on the stream.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// stream.send_payload(Bytes::from("Hello")).await?;
    /// ```
    pub async fn send_payload(&mut self, payload: Bytes) -> Result<(), ManiStreamError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_sender
            .send(ManiStreamCommand::SendPayload {
                payload,
                response: response_tx,
            })
            .await
            .map_err(|_| ManiStreamError::StreamClosed)?;
        response_rx
            .await
            .map_err(|_| ManiStreamError::StreamClosed)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_transitions_pattern() {
        let initial_role = ManiStreamRole::Unknown;
        assert!(matches!(initial_role, ManiStreamRole::Unknown));

        let sender_role = ManiStreamRole::Sender {
            retransmission_buffer: Arc::new(DashMap::new()),
            transfer_end_ack_sender: None,
        };
        assert!(matches!(sender_role, ManiStreamRole::Sender { .. }));

        let (dg_tx, _) = tokio::sync::mpsc::channel(1);
        let (cmd_tx, _) = tokio::sync::mpsc::channel(1);

        let receiver_role = ManiStreamRole::Receiver {
            retrans_packet_sender: dg_tx,
            pipeline_command_sender: Some(cmd_tx),
            initial_sequence_number: None,
        };
        assert!(matches!(receiver_role, ManiStreamRole::Receiver { .. }));
    }
}
