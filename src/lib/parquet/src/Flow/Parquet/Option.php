<?php declare(strict_types=1);

namespace Flow\Parquet;

enum Option
{
    /**
     * Some parquet writers might not properly use LogicalTyp for storing Strings or JSON's.
     * This option would tell the reader to treat all BYTE_ARRAY's as UTF-8 strings.
     *
     * Default value is true;
     */
    case BYTE_ARRAY_TO_STRING;

    /**
     * Whenever cardinality ration of the dictionary goes below this value, PagesBuilders is going to fallback to PLAIN encoding.
     * Cardinality ration is calculated as distinct values / total values.
     * Please notice that even when cardinality ration is above this value, PageBuilder will still fallback to PLAIN encoding
     * when dictionary size gets above DICTIONARY_PAGE_SIZE.
     *
     * Default value 0.4 (40% of the total values is distinct)
     */
    case DICTIONARY_PAGE_MIN_CARDINALITY_RATION;

    /**
     * Whenever size of the dictionary goes above this value, PagesBuilders is going to fallback to PLAIN encoding.
     *
     * Default value is 1Mb
     */
    case DICTIONARY_PAGE_SIZE;

    /**
     * Compression level for GZIP codec. This option is going to be passed to gzcompress function when Compression is set to GZIP.
     * Lower level means faster compression, but bigger file size.
     *
     * Default value is 9
     */
    case GZIP_COMPRESSION_LEVEL;

    /**
     * When this option is set to true, reader will try to convert INT96 logical type to DateTimeImmutable object.
     * Some parquet writers due to historical reasons might still use INT96 to store timestamps with nanoseconds precision
     * instead of using TIMESTAMP logical type.
     * Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true,
     * reader will round nanoseconds to microseconds.
     *
     * INT96 in general is not supported anymore, this option should be set to true by default, otherwise it will
     * return array of bytes (12) that represents INT96.
     *
     * Default value is true
     */
    case INT_96_AS_DATETIME;

    /**
     * PageBuilder is going to use this value to determine how many rows should be stored in one page.
     * PageBuilder is not going to make it precisely equal to this value, but it will try to make it as close as possible.
     * This should be considered as a threshold rather than a strict value.
     *
     * Default value is 128Mb
     *
     * https://parquet.apache.org/docs/file-format/configurations/#data-page--size
     */
    case PAGE_SIZE_BYTES;

    /**
     * Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true,
     * reader will round nanoseconds to microseconds.
     *
     * Default value is false
     */
    case ROUND_NANOSECONDS;

    /**
     * RowGroupBuilder is going to use this value to determine for how long it should keep adding rows to the buffer
     * before flushing it on disk.
     *
     * Default value is 8Kb
     *
     * https://parquet.apache.org/docs/file-format/configurations/#row-group-size
     */
    case ROW_GROUP_SIZE_BYTES;

    /**
     * RowGroupBuilder is going to use this value to determine how often it should check if RowGroup size is not exceeded.
     * This is a performance optimization, since checking RowGroup size is a costly operation.
     * If the value is set to 1000, RowGroupBuilder is going to check the size only after adding 1000 rows to the buffer.
     *
     * Default value is 1000
     */
    case ROW_GROUP_SIZE_CHECK_INTERVAL;
}
