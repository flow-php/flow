namespace cpp parquet
namespace java org.apache.parquet.format
namespace php Flow.Parquet.Thrift

enum Type {
  BOOLEAN = 0;
  INT32 = 1;
  INT64 = 2;
  INT96 = 3;
  FLOAT = 4;
  DOUBLE = 5;
  BYTE_ARRAY = 6;
  FIXED_LEN_BYTE_ARRAY = 7;
}

enum ConvertedType {
  UTF8 = 0;
  MAP = 1;
  MAP_KEY_VALUE = 2;
  LIST = 3;
  ENUM = 4;
  DECIMAL = 5;
  DATE = 6;
  TIME_MILLIS = 7;
  TIME_MICROS = 8;
  TIMESTAMP_MILLIS = 9;
  TIMESTAMP_MICROS = 10;
  UINT_8 = 11;
  UINT_16 = 12;
  UINT_32 = 13;
  UINT_64 = 14;
  INT_8 = 15;
  INT_16 = 16;
  INT_32 = 17;
  INT_64 = 18;
  JSON = 19;
  BSON = 20;
  INTERVAL = 21;
}

enum FieldRepetitionType {
  REQUIRED = 0;
  OPTIONAL = 1;
  REPEATED = 2;
}

struct Statistics {
   1: optional binary max;
   2: optional binary min;
   3: optional i64 null_count;
   4: optional i64 distinct_count;
   5: optional binary max_value;
   6: optional binary min_value;
}

struct StringType {}
struct UUIDType {}
struct MapType {}
struct ListType {}
struct EnumType {}
struct DateType {}

struct NullType {}

struct DecimalType {
  1: required i32 scale
  2: required i32 precision
}

struct MilliSeconds {}
struct MicroSeconds {}
struct NanoSeconds {}
union TimeUnit {
  1: MilliSeconds MILLIS
  2: MicroSeconds MICROS
  3: NanoSeconds NANOS
}

struct TimestampType {
  1: required bool isAdjustedToUTC
  2: required TimeUnit unit
}

struct TimeType {
  1: required bool isAdjustedToUTC
  2: required TimeUnit unit
}

struct IntType {
  1: required i8 bitWidth
  2: required bool isSigned
}

struct JsonType {
}

struct BsonType {
}

union LogicalType {
  1:  StringType STRING
  2:  MapType MAP
  3:  ListType LIST
  4:  EnumType ENUM
  5:  DecimalType DECIMAL
  6:  DateType DATE
  7:  TimeType TIME
  8:  TimestampType TIMESTAMP
  10: IntType INTEGER
  11: NullType UNKNOWN
  12: JsonType JSON
  13: BsonType BSON
  14: UUIDType UUID
}

struct SchemaElement {
  1: optional Type type;
  2: optional i32 type_length;
  3: optional FieldRepetitionType repetition_type;
  4: required string name;
  5: optional i32 num_children;
  6: optional ConvertedType converted_type;
  7: optional i32 scale
  8: optional i32 precision
  9: optional i32 field_id;
  10: optional LogicalType logicalType
}

enum Encoding {
  PLAIN = 0;
  PLAIN_DICTIONARY = 2;
  RLE = 3;
  BIT_PACKED = 4;
  DELTA_BINARY_PACKED = 5;
  DELTA_LENGTH_BYTE_ARRAY = 6;
  DELTA_BYTE_ARRAY = 7;
  RLE_DICTIONARY = 8;
  BYTE_STREAM_SPLIT = 9;
}

enum CompressionCodec {
  UNCOMPRESSED = 0;
  SNAPPY = 1;
  GZIP = 2;
  LZO = 3;
  BROTLI = 4;
  LZ4 = 5;
  ZSTD = 6;
  LZ4_RAW = 7;
}

enum PageType {
  DATA_PAGE = 0;
  INDEX_PAGE = 1;
  DICTIONARY_PAGE = 2;
  DATA_PAGE_V2 = 3;
}

enum BoundaryOrder {
  UNORDERED = 0;
  ASCENDING = 1;
  DESCENDING = 2;
}

struct DataPageHeader {
  1: required i32 num_values
  2: required Encoding encoding
  3: required Encoding definition_level_encoding;
  4: required Encoding repetition_level_encoding;
  5: optional Statistics statistics;
}

struct IndexPageHeader {
}

struct DictionaryPageHeader {
  1: required i32 num_values;
  2: required Encoding encoding
  3: optional bool is_sorted;
}

struct DataPageHeaderV2 {
  1: required i32 num_values
  2: required i32 num_nulls
  3: required i32 num_rows
  4: required Encoding encoding
  5: required i32 definition_levels_byte_length;
  6: required i32 repetition_levels_byte_length;
  7: optional bool is_compressed = true;
  8: optional Statistics statistics;
}

struct SplitBlockAlgorithm {}

union BloomFilterAlgorithm {
  1: SplitBlockAlgorithm BLOCK;
}

struct XxHash {}

union BloomFilterHash {
  1: XxHash XXHASH;
}

struct Uncompressed {}
union BloomFilterCompression {
  1: Uncompressed UNCOMPRESSED;
}

struct BloomFilterHeader {
  1: required i32 numBytes;
  2: required BloomFilterAlgorithm algorithm;
  3: required BloomFilterHash hash;
  4: required BloomFilterCompression compression;
}

struct PageHeader {
  1: required PageType type
  2: required i32 uncompressed_page_size
  3: required i32 compressed_page_size
  4: optional i32 crc
  5: optional DataPageHeader data_page_header;
  6: optional IndexPageHeader index_page_header;
  7: optional DictionaryPageHeader dictionary_page_header;
  8: optional DataPageHeaderV2 data_page_header_v2;
}

 struct KeyValue {
  1: required string key
  2: optional string value
}

struct SortingColumn {
  1: required i32 column_idx
  2: required bool descending
  3: required bool nulls_first
}

struct PageEncodingStats {
  1: required PageType page_type;
  2: required Encoding encoding;
  3: required i32 count;
}

struct ColumnMetaData {
  1: required Type type
  2: required list<Encoding> encodings
  3: required list<string> path_in_schema
  4: required CompressionCodec codec
  5: required i64 num_values
  6: required i64 total_uncompressed_size
  7: required i64 total_compressed_size
  8: optional list<KeyValue> key_value_metadata
  9: required i64 data_page_offset
  10: optional i64 index_page_offset
  11: optional i64 dictionary_page_offset
  12: optional Statistics statistics;
  13: optional list<PageEncodingStats> encoding_stats;
  14: optional i64 bloom_filter_offset;
  15: optional i32 bloom_filter_length;
}

struct EncryptionWithFooterKey {
}

struct EncryptionWithColumnKey {
  1: required list<string> path_in_schema
  2: optional binary key_metadata
}

union ColumnCryptoMetaData {
  1: EncryptionWithFooterKey ENCRYPTION_WITH_FOOTER_KEY
  2: EncryptionWithColumnKey ENCRYPTION_WITH_COLUMN_KEY
}

struct ColumnChunk {
  1: optional string file_path
  2: required i64 file_offset
  3: optional ColumnMetaData meta_data
  4: optional i64 offset_index_offset
  5: optional i32 offset_index_length
  6: optional i64 column_index_offset
  7: optional i32 column_index_length
  8: optional ColumnCryptoMetaData crypto_metadata
  9: optional binary encrypted_column_metadata
}

struct RowGroup {
  1: required list<ColumnChunk> columns
  2: required i64 total_byte_size
  3: required i64 num_rows
  4: optional list<SortingColumn> sorting_columns
  5: optional i64 file_offset
  6: optional i64 total_compressed_size
  7: optional i16 ordinal
}

struct TypeDefinedOrder {}

union ColumnOrder {
  1: TypeDefinedOrder TYPE_ORDER;
}

struct PageLocation {
  1: required i64 offset
  2: required i32 compressed_page_size
  3: required i64 first_row_index
}

struct OffsetIndex {
  1: required list<PageLocation> page_locations
}

struct ColumnIndex {
  1: required list<bool> null_pages
  2: required list<binary> min_values
  3: required list<binary> max_values
  4: required BoundaryOrder boundary_order
  5: optional list<i64> null_counts
}

struct AesGcmV1 {
  1: optional binary aad_prefix
  2: optional binary aad_file_unique
  3: optional bool supply_aad_prefix
}

struct AesGcmCtrV1 {
  1: optional binary aad_prefix
  2: optional binary aad_file_unique
  3: optional bool supply_aad_prefix
}

union EncryptionAlgorithm {
  1: AesGcmV1 AES_GCM_V1
  2: AesGcmCtrV1 AES_GCM_CTR_V1
}

struct FileMetaData {
  1: required i32 version
  2: required list<SchemaElement> schema;
  3: required i64 num_rows
  4: required list<RowGroup> row_groups
  5: optional list<KeyValue> key_value_metadata
  6: optional string created_by
  7: optional list<ColumnOrder> column_orders;
  8: optional EncryptionAlgorithm encryption_algorithm
  9: optional binary footer_signing_key_metadata
}

struct FileCryptoMetaData {
  1: required EncryptionAlgorithm encryption_algorithm
  2: optional binary key_metadata
}