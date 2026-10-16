<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class FlatColumnMother
{
    public static function byteArrayDecimal(): FlatColumn
    {
        return new FlatColumn('d', PhysicalType::BYTE_ARRAY, ConvertedType::DECIMAL, LogicalType::decimal(2, 30));
    }

    public static function flbaDecimal(): FlatColumn
    {
        return new FlatColumn(
            'd',
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ConvertedType::DECIMAL,
            LogicalType::decimal(2, 4),
            typeLength: 2,
        );
    }

    public static function float16(): FlatColumn
    {
        return new FlatColumn(
            'f',
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            logicalType: new LogicalType(LogicalType::FLOAT16),
            typeLength: 2,
        );
    }

    public static function int64Decimal(): FlatColumn
    {
        return new FlatColumn('d', PhysicalType::INT64, ConvertedType::DECIMAL, LogicalType::decimal(2, 10));
    }

    public static function int96(): FlatColumn
    {
        return new FlatColumn('t', PhysicalType::INT96);
    }

    public static function interval(): FlatColumn
    {
        return new FlatColumn('i', PhysicalType::FIXED_LEN_BYTE_ARRAY, ConvertedType::INTERVAL, typeLength: 12);
    }

    public static function uint32(): FlatColumn
    {
        return new FlatColumn('u', PhysicalType::INT32, logicalType: LogicalType::integer(32, false));
    }

    public static function uint8(): FlatColumn
    {
        return new FlatColumn('u', PhysicalType::INT32, logicalType: LogicalType::integer(8, false));
    }

    public static function uint64(): FlatColumn
    {
        return new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false));
    }
}
