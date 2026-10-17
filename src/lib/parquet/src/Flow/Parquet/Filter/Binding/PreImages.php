<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\PreImage\BooleanPreImage;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DatePreImage;
use Flow\Parquet\Filter\Binding\PreImage\DecimalPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DoublePreImage;
use Flow\Parquet\Filter\Binding\PreImage\Float32PreImage;
use Flow\Parquet\Filter\Binding\PreImage\IntegerPreImage;
use Flow\Parquet\Filter\Binding\PreImage\TimestampPreImage;
use Flow\Parquet\Filter\Binding\PreImage\UnsignedInt64PreImage;
use Flow\Parquet\Filter\Binding\PreImage\UuidPreImage;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int32DateConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedIntegerConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final readonly class PreImages
{
    /**
     * The pre-image of the column's decoded values, from the converter that decodes them; null when no literal kind
     * compares with them exactly.
     */
    public static function forColumn(FlatColumn $column, ?Converter $converter): ?PreImage
    {
        return match (true) {
            $converter === null => self::physical($column),
            $converter instanceof Int32DateConverter => new DatePreImage(),
            $converter instanceof Int64DateTimeConverter => TimestampPreImage::forColumn($column),
            $converter instanceof DecimalConverter => DecimalPreImage::forColumn($column),
            $converter instanceof UuidConverter => new UuidPreImage($converter),
            $converter instanceof UnsignedIntegerConverter => new IntegerPreImage(0, 2 ** 32 - 1),
            $converter instanceof UnsignedInt64Converter => new UnsignedInt64PreImage($converter),
            default => null,
        };
    }

    private static function physical(FlatColumn $column): ?PreImage
    {
        $logical = $column->logicalType();

        return match ($column->type()) {
            PhysicalType::BOOLEAN => new BooleanPreImage(),
            PhysicalType::INT32, PhysicalType::INT64 => IntegerPreImage::forColumn($column),
            PhysicalType::DOUBLE => new DoublePreImage(),
            PhysicalType::FLOAT => new Float32PreImage(),
            PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY => PhysicalOrder::forColumn($column)
                === PhysicalOrder::Bytes
                && ($logical === null || $logical->is(LogicalType::STRING) || $logical->is(LogicalType::ENUM))
                    ? new BytesPreImage()
                    : null,
            PhysicalType::INT96 => null,
        };
    }
}
