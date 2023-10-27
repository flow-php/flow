<?php declare(strict_types=1);

namespace Flow\Parquet;

enum Option
{
    /**
     * Some parquet writers might not properly use LogicalTyp for storing Strings or JSON's.
     * This option would tell the reader to treat all BYTE_ARRAY's as UTF-8 strings.
     */
    case BYTE_ARRAY_TO_STRING;

    /**
     * When this option is set to true, reader will try to convert INT96 logical type to DateTimeImmutable object.
     * Some parquet writers due to historical reasons might still use INT96 to store timestamps with nanoseconds precision
     * instead of using TIMESTAMP logical type.
     * Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true,
     * reader will round nanoseconds to microseconds.
     *
     * INT96 in general is not supported anymore, this option should be set to true by default, otherwise it will
     * return array of bytes (12) that represents INT96.
     */
    case INT_96_AS_DATETIME;

    /**
     * PageBuilder is going to use this value to determine how many rows should be stored in one page.
     * PageBuilder is not going to make it precisely equal to this value, but it will try to make it as close as possible.
     * This should be considered as a threshold rather than a strict value.
     *
     * https://parquet.apache.org/docs/file-format/configurations/#data-page--size
     */
    case PAGE_SIZE_BYTES;

    /**
     * Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true,
     * reader will round nanoseconds to microseconds.
     */
    case ROUND_NANOSECONDS;

    /**
     * RowGroupBuilder is going to use this value to determine for how long it should keep adding rows to the buffer
     * before flushing it on disk.
     *
     * https://parquet.apache.org/docs/file-format/configurations/#row-group-size
     */
    case ROW_GROUP_SIZE_BYTES;
}
