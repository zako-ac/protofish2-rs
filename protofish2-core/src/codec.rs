use bytes::{Buf, BufMut};
use thiserror::Error;

pub mod primitives;
pub mod varint;

#[derive(Error, Debug, Copy, Clone, Eq, PartialEq)]
#[error("unexpected end of buffer")]
pub struct UnexpectedEnd;

pub type CodecResult<T> = Result<T, UnexpectedEnd>;

pub trait Codec: Sized {
    fn encode<B: BufMut>(&self, buf: &mut B);
    fn decode<B: Buf>(buf: &mut B) -> CodecResult<Self>;
}
