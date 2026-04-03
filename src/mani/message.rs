use bytes::Bytes;

use crate::{SequenceNumber, Timestamp, compression::CompressionType};

#[derive(Debug, Clone)]
pub struct ManiHello {
    pub compression_type: CompressionType,
}

#[derive(Debug, Clone)]
pub struct ManiPayload {
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct ManiNack {
    pub sequence_numbers: Vec<SequenceNumber>,
}

#[derive(Debug, Clone)]
pub struct ManiRetrans {
    pub sequence_number: SequenceNumber,
    pub timestamp: Timestamp,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransferMode {
    Dual = 0x00,
    UnreliableOnly = 0x01,
}

impl TransferMode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(TransferMode::Dual),
            0x01 => Some(TransferMode::UnreliableOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferStart {
    pub mode: TransferMode,
    pub compression_type: CompressionType,
    pub initial_sequence_number: SequenceNumber,
    pub data_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TransferCreditsUpdate {
    pub additional_credits: usize,
}

#[derive(Debug, Clone)]
pub struct TransferEnd {
    pub final_sequence_number: SequenceNumber,
}

#[derive(Debug, Clone)]
pub struct TransferError {
    pub error_code: TransferErrorCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TransferErrorCode {
    UnsupportedCompression = 0x0001,
    InternalError = 0x0002,
}

impl TransferErrorCode {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x0001 => Some(TransferErrorCode::UnsupportedCompression),
            0x0002 => Some(TransferErrorCode::InternalError),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ManiMessage {
    Hello(ManiHello),
    Payload(ManiPayload),
    Nack(ManiNack),
    Retrans(ManiRetrans),
    TransferStart(TransferStart),
    TransferEnd(TransferEnd),
    TransferEndAck,
    TransferError(TransferError),
    EndOfStream,
    TransferCreditsUpdate(TransferCreditsUpdate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ManiMessageType {
    Hello = 0x00,
    Payload = 0x01,
    Nack = 0x02,
    Retrans = 0x03,
    TransferStart = 0x04,
    TransferEnd = 0x05,
    TransferEndAck = 0x06,
    TransferError = 0x07,
    EndOfStream = 0x08,
    TransferCreditsUpdate = 0x09,
}

impl ManiMessageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(ManiMessageType::Hello),
            0x01 => Some(ManiMessageType::Payload),
            0x02 => Some(ManiMessageType::Nack),
            0x03 => Some(ManiMessageType::Retrans),
            0x04 => Some(ManiMessageType::TransferStart),
            0x05 => Some(ManiMessageType::TransferEnd),
            0x06 => Some(ManiMessageType::TransferEndAck),
            0x07 => Some(ManiMessageType::TransferError),
            0x08 => Some(ManiMessageType::EndOfStream),
            0x09 => Some(ManiMessageType::TransferCreditsUpdate),
            _ => None,
        }
    }
}

impl ManiMessage {
    #[allow(dead_code)]
    pub fn message_type(&self) -> ManiMessageType {
        match self {
            ManiMessage::Hello(_) => ManiMessageType::Hello,
            ManiMessage::Payload(_) => ManiMessageType::Payload,
            ManiMessage::Nack(_) => ManiMessageType::Nack,
            ManiMessage::Retrans(_) => ManiMessageType::Retrans,
            ManiMessage::TransferStart(_) => ManiMessageType::TransferStart,
            ManiMessage::TransferEnd(_) => ManiMessageType::TransferEnd,
            ManiMessage::TransferEndAck => ManiMessageType::TransferEndAck,
            ManiMessage::TransferError(_) => ManiMessageType::TransferError,
            ManiMessage::EndOfStream => ManiMessageType::EndOfStream,
            ManiMessage::TransferCreditsUpdate(_) => ManiMessageType::TransferCreditsUpdate,
        }
    }
}
