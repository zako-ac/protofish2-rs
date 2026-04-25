use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    ManiStreamId,
    mani::{codec, message::ManiMessage},
};

pub struct ManiReadFrame<R: AsyncRead + Unpin> {
    pub stream_id: ManiStreamId,

    stream: FramedRead<R, LengthDelimitedCodec>,
}

impl<R: AsyncRead + Unpin> ManiReadFrame<R> {
    pub fn new(stream_id: ManiStreamId, read_stream: R) -> Self {
        Self {
            stream_id,
            stream: FramedRead::new(read_stream, LengthDelimitedCodec::new()),
        }
    }

    pub async fn read_frame(&mut self) -> Option<ManiMessage> {
        let bytes = self
            .stream
            .next()
            .await
            .transpose()
            .ok()
            .flatten()
            .map(|bytes| bytes.freeze())?;

        codec::parse_mani_message(bytes)
            .inspect_err(|err| {
                tracing::error!(
                    "Failed to parse ManiMessage from stream {}: {}",
                    self.stream_id,
                    err
                );
            })
            .ok()
    }
}

pub struct ManiWriteFrame<W: AsyncWrite + Unpin> {
    stream: FramedWrite<W, LengthDelimitedCodec>,
}

impl<W: AsyncWrite + Unpin> ManiWriteFrame<W> {
    pub fn new(write_stream: W) -> Self {
        Self {
            stream: FramedWrite::new(write_stream, LengthDelimitedCodec::new()),
        }
    }

    pub async fn write_frame(&mut self, message: &ManiMessage) -> Result<(), std::io::Error> {
        let serialized = codec::serialize_mani_message(message);
        self.stream
            .send(serialized)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(())
    }

    pub async fn close(&mut self) {
        let _ = self.stream.close().await;
    }
}
