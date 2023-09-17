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
     * Since PHP does not support nanoseconds precision for DateTime objects, when this options is set to true,
     * reader will round nanoseconds to microseconds.
     */
    case ROUND_NANOSECONDS;
}
