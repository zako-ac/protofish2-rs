use std::io::Read;

/// Supported compression algorithms for data transfers.
///
/// # Examples
///
/// ```
/// use protofish2::compression::CompressionType;
///
/// let comp = CompressionType::Gzip;
/// println!("Using compression: {:?}", comp);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression - data is sent as-is
    None = 0,

    /// Gzip compression (RFC 1952)
    Gzip = 1,

    /// Zstandard compression
    Zstd = 2,

    /// LZ4 compression
    Lz4 = 3,
}

impl CompressionType {
    /// Converts a byte value to a `CompressionType`.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Gzip),
            2 => Some(CompressionType::Zstd),
            3 => Some(CompressionType::Lz4),
            _ => None,
        }
    }

    /// Creates a boxed compression implementation for the selected type.
    pub(crate) fn to_boxed_compression(self) -> Option<Box<dyn Compression>> {
        match self {
            CompressionType::None => Some(Box::new(NoCompression)),
            CompressionType::Gzip => Some(Box::new(GzipCompression)),
            CompressionType::Zstd => Some(Box::new(ZstdCompression)),
            CompressionType::Lz4 => Some(Box::new(Lz4Compression)),
        }
    }
}

/// Internal trait for compression implementations.
#[doc(hidden)]
pub trait Compression: Send + Sync {
    fn compression_type(&self) -> CompressionType;

    fn compress(&self, data: &[u8]) -> Vec<u8>;
    fn decompress(&self, data: &[u8]) -> Vec<u8>;
}

#[derive(Debug, Clone, Copy)]
struct NoCompression;

impl Compression for NoCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::None
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }
}

#[derive(Debug, Clone, Copy)]
struct GzipCompression;

impl Compression for GzipCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Gzip
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::read::GzEncoder::new(data, flate2::Compression::default());
        let mut out = Vec::new();
        let _ = encoder.read_to_end(&mut out);
        out
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut out = Vec::new();
        let _ = decoder.read_to_end(&mut out);
        out
    }
}

#[derive(Debug, Clone, Copy)]
struct ZstdCompression;

impl Compression for ZstdCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Zstd
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 0).unwrap_or_default()
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        zstd::decode_all(data).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy)]
struct Lz4Compression;

impl Compression for Lz4Compression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Lz4
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        lz4_flex::compress_prepend_size(data)
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(data).unwrap_or_default()
    }
}
