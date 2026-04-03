use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut, TryGetError};
use thiserror::Error;

use crate::mani::message::{
    ManiHello, ManiMessage, ManiMessageType, ManiNack, ManiPayload, ManiRetrans, TransferEnd,
    TransferStart,
};
use crate::{SequenceNumber, Timestamp};

#[derive(Error, Debug)]
pub enum ManiMessageParseError {
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),

    #[error("Invalid message format")]
    InvalidMessageFormat,

    #[error("Unexpected end of message")]
    UnexpectedEndOfMessage(#[from] TryGetError),
}

pub fn parse_mani_message(bytes: Bytes) -> Result<ManiMessage, ManiMessageParseError> {
    let mut cur = Cursor::new(bytes);

    let message_type = cur.try_get_u8()?;

    match ManiMessageType::from_u8(message_type)
        .ok_or(ManiMessageParseError::InvalidMessageType(message_type))?
    {
        ManiMessageType::Hello => parse_hello_message(&mut cur),
        ManiMessageType::Payload => parse_payload_message(&mut cur),
        ManiMessageType::Nack => parse_nack_message(&mut cur),
        ManiMessageType::Retrans => parse_retrans_message(&mut cur),
        ManiMessageType::TransferStart => parse_transfer_start_message(&mut cur),
        ManiMessageType::TransferEnd => parse_transfer_end_message(&mut cur),
        ManiMessageType::TransferEndAck => Ok(ManiMessage::TransferEndAck),
        ManiMessageType::TransferError => parse_transfer_error_message(&mut cur),
        ManiMessageType::EndOfStream => Ok(ManiMessage::EndOfStream),
    }
}

pub fn serialize_mani_message(message: &ManiMessage) -> Bytes {
    let mut buf = BytesMut::new();

    match message {
        ManiMessage::Hello(hello) => serialize_hello_message(&mut buf, hello),
        ManiMessage::Payload(payload) => serialize_payload_message(&mut buf, payload),
        ManiMessage::Nack(nack) => serialize_nack_message(&mut buf, nack),
        ManiMessage::Retrans(retrans) => serialize_retrans_message(&mut buf, retrans),
        ManiMessage::TransferStart(transfer_start) => {
            serialize_transfer_start_message(&mut buf, transfer_start)
        }
        ManiMessage::TransferEnd(transfer_end) => {
            serialize_transfer_end_message(&mut buf, transfer_end)
        }
        ManiMessage::TransferEndAck => {
            buf.put_u8(ManiMessageType::TransferEndAck as u8);
        }
        ManiMessage::TransferError(transfer_error) => {
            buf.put_u8(ManiMessageType::TransferError as u8);
            buf.put_u16(transfer_error.error_code as u16);
        }
        ManiMessage::EndOfStream => {
            buf.put_u8(ManiMessageType::EndOfStream as u8);
        }
    }

    buf.freeze()
}

fn parse_hello_message(cur: &mut Cursor<Bytes>) -> Result<ManiMessage, ManiMessageParseError> {
    let compression_type = cur.try_get_u8()?;

    Ok(ManiMessage::Hello(crate::mani::message::ManiHello {
        compression_type: crate::compression::CompressionType::from_u8(compression_type)
            .ok_or(ManiMessageParseError::InvalidMessageFormat)?,
    }))
}

fn parse_payload_message(cur: &mut Cursor<Bytes>) -> Result<ManiMessage, ManiMessageParseError> {
    let remaining = cur.remaining();
    let payload = cur.copy_to_bytes(remaining);

    Ok(ManiMessage::Payload(ManiPayload { payload }))
}

fn parse_nack_message(cur: &mut Cursor<Bytes>) -> Result<ManiMessage, ManiMessageParseError> {
    let count = cur.try_get_u32()? as usize;
    let mut sequence_numbers = Vec::with_capacity(count);

    for _ in 0..count {
        let seq_num = cur.try_get_u32()?;
        sequence_numbers.push(SequenceNumber(seq_num));
    }

    Ok(ManiMessage::Nack(ManiNack { sequence_numbers }))
}

fn parse_retrans_message(cur: &mut Cursor<Bytes>) -> Result<ManiMessage, ManiMessageParseError> {
    let sequence_number = SequenceNumber(cur.try_get_u32()?);
    let timestamp = Timestamp(cur.try_get_u64()?);
    let remaining = cur.remaining();
    let payload = cur.copy_to_bytes(remaining);

    Ok(ManiMessage::Retrans(ManiRetrans {
        sequence_number,
        timestamp,
        payload,
    }))
}

fn parse_transfer_start_message(
    cur: &mut Cursor<Bytes>,
) -> Result<ManiMessage, ManiMessageParseError> {
    let mode_val = cur.try_get_u8()?;
    let compression_type = cur.try_get_u8()?;
    let initial_sequence_number = SequenceNumber(cur.try_get_u32()?);
    let has_data_size = cur.try_get_u8()? != 0;
    let data_size = if has_data_size {
        Some(cur.try_get_u64()?)
    } else {
        None
    };

    Ok(ManiMessage::TransferStart(TransferStart {
        mode: crate::mani::message::TransferMode::from_u8(mode_val)
            .ok_or(ManiMessageParseError::InvalidMessageFormat)?,
        compression_type: crate::compression::CompressionType::from_u8(compression_type)
            .ok_or(ManiMessageParseError::InvalidMessageFormat)?,
        initial_sequence_number,
        data_size,
    }))
}

fn parse_transfer_end_message(
    cur: &mut Cursor<Bytes>,
) -> Result<ManiMessage, ManiMessageParseError> {
    let final_sequence_number = SequenceNumber(cur.try_get_u32()?);

    Ok(ManiMessage::TransferEnd(TransferEnd {
        final_sequence_number,
    }))
}

fn parse_transfer_error_message(
    cur: &mut Cursor<Bytes>,
) -> Result<ManiMessage, ManiMessageParseError> {
    let error_code = cur.try_get_u16()?;

    let error_code = crate::mani::message::TransferErrorCode::from_u16(error_code)
        .ok_or(ManiMessageParseError::InvalidMessageFormat)?;

    Ok(ManiMessage::TransferError(
        crate::mani::message::TransferError { error_code },
    ))
}

fn serialize_hello_message(buf: &mut BytesMut, hello: &ManiHello) {
    buf.put_u8(ManiMessageType::Hello as u8);
    buf.put_u8(hello.compression_type as u8);
}

fn serialize_payload_message(buf: &mut BytesMut, payload: &ManiPayload) {
    buf.put_u8(ManiMessageType::Payload as u8);
    buf.put_slice(&payload.payload);
}

fn serialize_nack_message(buf: &mut BytesMut, nack: &ManiNack) {
    buf.put_u8(ManiMessageType::Nack as u8);
    buf.put_u32(nack.sequence_numbers.len() as u32);
    for seq_num in &nack.sequence_numbers {
        buf.put_u32(seq_num.0);
    }
}

fn serialize_retrans_message(buf: &mut BytesMut, retrans: &ManiRetrans) {
    buf.put_u8(ManiMessageType::Retrans as u8);
    buf.put_u32(retrans.sequence_number.0);
    buf.put_u64(retrans.timestamp.0);
    buf.put_slice(&retrans.payload);
}

fn serialize_transfer_start_message(buf: &mut BytesMut, transfer_start: &TransferStart) {
    buf.put_u8(ManiMessageType::TransferStart as u8);
    buf.put_u8(transfer_start.mode as u8);
    buf.put_u8(transfer_start.compression_type as u8);
    buf.put_u32(transfer_start.initial_sequence_number.0);
    if let Some(data_size) = transfer_start.data_size {
        buf.put_u8(1);
        buf.put_u64(data_size);
    } else {
        buf.put_u8(0);
    }
}

fn serialize_transfer_end_message(buf: &mut BytesMut, transfer_end: &TransferEnd) {
    buf.put_u8(ManiMessageType::TransferEnd as u8);
    buf.put_u32(transfer_end.final_sequence_number.0);
}
