#[cfg(feature = "jitter")]
use crate::mani::transfer::recv::TransferUnreliableRecvStream;
#[cfg(feature = "jitter")]
use bytes::Bytes;
#[cfg(feature = "jitter")]
use std::collections::BTreeMap;

#[cfg(feature = "jitter")]
pub struct OpusJitterBuffer {
    receiver: TransferUnreliableRecvStream,
    decoder: opus::Decoder,
    buffer: BTreeMap<u64, Bytes>,
    frame_size_ms: u64,
    playout_delay_ms: u64,
    next_play_ts: Option<u64>,
    channels: opus::Channels,
    is_eof: bool,
}

#[cfg(feature = "jitter")]
impl OpusJitterBuffer {
    pub fn new(
        receiver: TransferUnreliableRecvStream,
        sample_rate: u32,
        channels: opus::Channels,
        frame_size_ms: u64,
        playout_delay_ms: u64,
    ) -> Result<Self, opus::Error> {
        let decoder = opus::Decoder::new(sample_rate, channels)?;
        Ok(Self {
            receiver,
            decoder,
            buffer: BTreeMap::new(),
            frame_size_ms,
            playout_delay_ms,
            next_play_ts: None,
            channels,
            is_eof: false,
        })
    }

    /// Yields the next decoded PCM frame.
    ///
    /// # Runtime requirement
    ///
    /// This method uses [`tokio::task::block_in_place`] internally and therefore
    /// **must be called from a multi-thread Tokio runtime** (the default for
    /// `#[tokio::main]`). It will panic if called from a `current_thread` runtime.
    pub async fn yield_pcm(&mut self) -> Result<Option<Vec<f32>>, opus::Error> {
        loop {
            // Buffer management
            if let Some(next_play_ts) = self.next_play_ts {
                let max_ts = self.buffer.keys().last().copied().unwrap_or(0);

                if self.buffer.contains_key(&next_play_ts) {
                    let chunk = self.buffer.remove(&next_play_ts).unwrap();
                    let max_samples = 5760 * self.channels as usize;
                    let mut pcm = vec![0f32; max_samples];
                    let decoded_len = tokio::task::block_in_place(|| {
                        self.decoder.decode_float(&chunk, &mut pcm, false)
                    })?;
                    pcm.truncate(decoded_len * self.channels as usize);
                    self.next_play_ts = Some(next_play_ts + self.frame_size_ms);
                    return Ok(Some(pcm));
                } else if self.is_eof {
                    if self.buffer.is_empty() {
                        return Ok(None); // Stop if EOF and empty
                    }
                    // Skip gap: set next_play_ts to the next available ts
                    self.next_play_ts = Some(*self.buffer.keys().next().unwrap());
                    continue;
                } else if max_ts.saturating_sub(next_play_ts) >= self.playout_delay_ms {
                    let max_samples = 5760 * self.channels as usize;
                    let mut pcm = vec![0f32; max_samples];
                    let decoded_len = tokio::task::block_in_place(|| {
                        self.decoder.decode_float(&[], &mut pcm, true)
                    })?;
                    pcm.truncate(decoded_len * self.channels as usize);
                    self.next_play_ts = Some(next_play_ts + self.frame_size_ms);
                    return Ok(Some(pcm));
                }
            }

            // Receive next chunk
            match self.receiver.recv().await {
                Some(chunk) => {
                    let ts = chunk.timestamp.0;
                    if self.next_play_ts.is_none() {
                        self.next_play_ts = Some(ts);
                    }
                    self.buffer.insert(ts, chunk.content);
                }
                None => {
                    self.is_eof = true;
                    // If no frames were ever received, there is nothing to play.
                    if self.next_play_ts.is_none() {
                        return Ok(None);
                    }
                }
            }
        }
    }
}
