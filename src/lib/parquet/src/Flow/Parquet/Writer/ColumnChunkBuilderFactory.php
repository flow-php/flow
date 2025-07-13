<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions, Encodings};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\{DeltaBinaryPackedColumnChunkBuilder, PlainFlatColumnChunkBuilder, RLEDictionaryChunkBuilder};

final class ColumnChunkBuilderFactory
{
    public static function createBuilder(FlatColumn $column, Options $options, Compressions $compressions) : ColumnChunkBuilder
    {
        if ($options->has(Option::COLUMNS_ENCODINGS)) {
            $columnsEncodings = $options->getColumnsEncodings();
            $flatPath = $column->flatPath();

            if ($columnsEncodings !== null && $columnsEncodings->hasFlatPath($flatPath)) {
                $encoding = $columnsEncodings->getEncodingForFlatPath($flatPath);

                if ($encoding !== null) {
                    return self::createForEncoding($column, $encoding, $options, $compressions);
                }
            }
        }

        if (($column->type() === PhysicalType::INT32 || $column->type() === PhysicalType::INT64) && $options->getInt(Option::WRITER_VERSION) === 2) {
            return new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compressions);
        }

        return new PlainFlatColumnChunkBuilder($column, $options, $compressions);
    }

    private static function createForEncoding(FlatColumn $column, Encodings $encoding, Options $options, Compressions $compressions) : ColumnChunkBuilder
    {
        self::validateEncodingForColumn($column, $encoding);

        return match ($encoding) {
            Encodings::PLAIN => new PlainFlatColumnChunkBuilder($column, $options, $compressions),
            Encodings::RLE_DICTIONARY => new RLEDictionaryChunkBuilder($column, $options, $compressions),
            Encodings::DELTA_BINARY_PACKED => new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compressions),
            default => throw new InvalidArgumentException("Unsupported encoding for column builder: {$encoding->name}"),
        };
    }

    private static function validateEncodingForColumn(FlatColumn $column, Encodings $encoding) : void
    {
        $columnType = $column->type();
        $encodingName = $encoding->name;
        $flatPath = $column->flatPath();

        switch ($encoding) {
            case Encodings::DELTA_BINARY_PACKED:
                if ($columnType !== PhysicalType::INT32 && $columnType !== PhysicalType::INT64) {
                    throw new InvalidArgumentException(
                        'DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64 columns. ' .
                        "Column '{$flatPath}' has type: {$columnType->name}"
                    );
                }

                break;

            case Encodings::RLE_DICTIONARY:
                if ($columnType === PhysicalType::FIXED_LEN_BYTE_ARRAY) {
                    throw new InvalidArgumentException(
                        'RLE_DICTIONARY encoding is not supported for FIXED_LEN_BYTE_ARRAY columns. ' .
                        "Column '{$flatPath}' has type: {$columnType->name}"
                    );
                }

                break;

            case Encodings::PLAIN:
                break;

            default:
                throw new InvalidArgumentException(
                    "Encoding '{$encodingName}' is not implemented. " .
                    'Supported encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED'
                );
        }
    }
}
