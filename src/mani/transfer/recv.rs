use std::sync::Arc;

use crate::{
    Chunk, ManiStreamId, SequenceNumber, datagram::packet::Packet,
    mani::transfer::assembler::Assembler,
};
use tokio::sync::{
    Notify,
    mpsc::{Receiver, Sender},
    oneshot,
};

pub(crate) enum RecvPipelineCommand {
    EndTransfer {
        final_sequence_number: SequenceNumber,
        reply: oneshot::Sender<()>,
    },
}

pub(crate) enum RecvSenderCommand {
    UpdateCredits {
        additional_credits: usize,
    },
    Nack {
        sequence_numbers: Vec<SequenceNumber>,
    },
}

pub struct TransferReliableRecvStream {
    pub id: ManiStreamId,

    receiver: Receiver<Packet>,
    assembler: Assembler,

    end_receiver: Arc<Notify>,
    sender_command_sender: Sender<RecvSenderCommand>,
    command_receiver: Receiver<RecvPipelineCommand>,
    pending_end: Option<(SequenceNumber, oneshot::Sender<()>)>,
}

impl TransferReliableRecvStream {
    pub(crate) fn new(
        id: ManiStreamId,
        receiver: Receiver<Packet>,
        max_retransmission_buffer_size: usize,
        end_receiver: Arc<Notify>,
        command_receiver: Receiver<RecvPipelineCommand>,
        sender_command_sender: Sender<RecvSenderCommand>,
    ) -> Self {
        Self {
            id,
            receiver,
            assembler: Assembler::new(max_retransmission_buffer_size),
            end_receiver,
            sender_command_sender,
            command_receiver,
            pending_end: None,
        }
    }

    pub async fn recv(&mut self) -> Option<Vec<Chunk>> {
        loop {
            #[allow(clippy::collapsible_if)]
            if let Some((final_seq, _)) = &self.pending_end {
                if self.assembler.cursor() > *final_seq {
                    let (_, reply) = self.pending_end.take().unwrap();
                    let _ = reply.send(());
                    return None; // Signal EOF
                }
            }

            tokio::select! {
                _ = self.end_receiver.notified() => {
                    return None; // Signal EOF
                }

                Some(cmd) = self.command_receiver.recv() => {
                    match cmd {
                        RecvPipelineCommand::EndTransfer { final_sequence_number, reply } => {
                            self.pending_end = Some((final_sequence_number, reply));
                        }
                    }
                }
                packet_opt = self.receiver.recv() => {
                    let packet = match packet_opt {
                        Some(c) => c,
                        None => return None,
                    };
                    let sequence_number = packet.sequence_number;
                    if let Err(err) = self.assembler.push(packet.sequence_number, packet) {
                        tracing::error!(
                            "Failed to push packet with sequence number {} to assembler: {}",
                            sequence_number,
                            err
                        );
                    }

                    let missings = self.assembler.missing_sequence_numbers();
                    #[allow(clippy::collapsible_if)]
                    if !missings.is_empty() {
                        let command = RecvSenderCommand::Nack { sequence_numbers: missings };
                        if let Err(err) = self.sender_command_sender.send(command).await {
                            tracing::trace!("Failed to send NACK for missing sequence numbers: {}", err);
                        }
                    }

                    let packets = self.assembler.read_ordered();
                    if !packets.is_empty() {
                        return Some(packets.into_iter().map(Into::into).collect());
                    }
                }
            }
        }
    }
}

pub struct TransferUnreliableRecvStream {
    pub id: ManiStreamId,

    end_receiver: Arc<Notify>,
    receiver: Receiver<Packet>,
}

impl TransferUnreliableRecvStream {
    pub(crate) async fn new(
        id: ManiStreamId,
        receiver: Receiver<Packet>,
        end_receiver: Arc<Notify>,
    ) -> Self {
        Self {
            id,
            receiver,
            end_receiver,
        }
    }

    pub async fn recv(&mut self) -> Option<Chunk> {
        tokio::select! {
            _ = self.end_receiver.notified() => {
                return None; // Signal EOF
            }
            packet_opt =  self.receiver.recv() => {
                let packet = match packet_opt {
                    Some(c) => c,
                    None => return None,
                };
                Some(packet.into())
            }
        }
    }
}

pub(crate) async fn create_stream_pair(
    id: ManiStreamId,
    receiver1: Receiver<Packet>,
    receiver2: Receiver<Packet>,
    end_receiver: Arc<Notify>,
    sender_command_sender: Sender<RecvSenderCommand>,
    max_retransmission_buffer_size: usize,
    command_receiver: Receiver<RecvPipelineCommand>,
) -> (TransferReliableRecvStream, TransferUnreliableRecvStream) {
    let reliable_stream = TransferReliableRecvStream::new(
        id,
        receiver1,
        max_retransmission_buffer_size,
        end_receiver.clone(),
        command_receiver,
        sender_command_sender.clone(),
    );
    let unreliable_stream = TransferUnreliableRecvStream::new(
        id,
        receiver2,
        end_receiver,
    )
    .await;

    (reliable_stream, unreliable_stream)
}
