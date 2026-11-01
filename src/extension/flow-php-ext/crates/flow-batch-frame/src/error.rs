//! Every refusal as data; flow_php renders the message its PHP twin throws.

use arrow_schema::{ArrowError, DataType};

/// The layout an offsets buffer belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Of {
    List,
    Map,
    Utf8,
}

/// The layout a fixed-width values buffer belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Values {
    Int64,
    Int32,
    Float64,
    Boolean,
    FixedBinary16,
}

/// The child of a list or map that holds nulls it may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    ListElement,
    MapKey,
    MapValue,
}

/// The part of a nested type JSON that is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Part {
    Element,
    Key,
    Value,
    Base,
}

/// `node` is a pre-order index in one column's node tree; `column` and `buffer` index a frame body.
#[derive(Debug)]
pub enum Error {
    SchemaJsonUtf8,
    SchemaJson(serde_json::Error),
    TypeJsonUtf8,
    TypeJson(serde_json::Error),
    MissingPart {
        type_name: String,
        part: Part,
    },
    UnknownType {
        type_name: String,
    },
    /// A list, map, structure or optional type JSON without the type it nests.
    MissingNestedType,
    /// An enum type JSON without its class.
    MissingEnumClass,
    BufferCount {
        node: usize,
        buffers: usize,
        expected: usize,
    },
    OffsetsLength {
        of: Of,
        bytes: u64,
        expected: u64,
        rows: u64,
    },
    MapEntriesValidity {
        bytes: u64,
    },
    ValidityTooShort {
        bytes: u64,
        rows: u64,
    },
    BuffersExhausted,
    LeftoverBuffers {
        count: usize,
    },
    NullKindNullCount {
        rows: u64,
        null_count: u64,
    },
    NullCountMismatch {
        declared: u64,
        derived: u64,
        rows: u64,
    },
    NestedNulls {
        node: usize,
        role: Role,
        nulls: u64,
    },
    OffsetsStart {
        of: Of,
        first: u64,
    },
    OffsetsNotMonotonic {
        of: Of,
        index: u64,
        previous: u64,
        current: u64,
    },
    ValuesLength {
        values: Values,
        bytes: u64,
        expected: u64,
        rows: u64,
    },
    Utf8DataLength {
        bytes: u64,
        last_offset: u64,
    },
    OffsetOverflow {
        last: u64,
    },
    DataTypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    Arrow(ArrowError),
    FrameTooLarge {
        bytes: u64,
    },
    DirectoryTruncated,
    FrameNodeCount {
        frame: u64,
        schema: u64,
    },
    FrameBufferCount {
        frame: u64,
        schema: u64,
    },
    ColumnRows {
        column: usize,
        rows: u64,
        frame_rows: u64,
    },
    ColumnNulls {
        column: usize,
        nulls: u64,
        rows: u64,
    },
    BufferOutsideArea {
        buffer: usize,
        area: u64,
    },
    BufferShorterThanPrefix {
        buffer: usize,
    },
    CompressedBuffer {
        buffer: usize,
        prefix: i64,
    },
    /// A codec's output is not as long as the buffer's prefix declares.
    DecodedLength {
        buffer: usize,
        decoded: u64,
        prefix: i64,
    },
    NodesDisagree {
        column: usize,
    },
    MalformedColumn {
        column: usize,
        error: Box<Error>,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}
