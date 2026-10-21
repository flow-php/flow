<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\FlatColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkBuilder\RLEDictionaryChunkBuilder;
use Flow\Parquet\Writer\ValueStorage\BooleanValueStorage;
use Flow\Parquet\Writer\ValueStorage\BufferValueStorage;
use Flow\Parquet\Writer\ValueStorage\DeltaBinaryPackedValueStorage;
use Flow\Parquet\Writer\ValueStorage\DeltaByteArrayValueStorage;
use Flow\Parquet\Writer\ValueStorage\DeltaLengthByteArrayValueStorage;
use Flow\Parquet\Writer\ValueStorage\RLEBooleanValueStorage;
use Flow\Parquet\Writer\ValueStorage\ValueStorage;

use function array_key_exists;

final class ColumnChunkBuilderFactory
{
    public static function createBuilder(
        FlatColumn $column,
        Options $options,
        Compressions $compressions,
        StatisticsOptions $statistics,
    ): ColumnChunkBuilder {
        $columnCompression = $compressions;
        $flatPath = $column->flatPath();

        if ($options->has(Option::COLUMNS_COMPRESSIONS)) {
            $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);

            if ($columnsCompressions !== null && array_key_exists($flatPath, $columnsCompressions)) {
                // Options::getArray returns array<mixed>; instanceof narrows below.
                // @mago-ignore analysis:mixed-assignment
                $compression = $columnsCompressions[$flatPath];

                if ($compression instanceof Compressions) {
                    $columnCompression = $compression;
                }
            }
        }

        $policy = new ColumnEncodingPolicy(
            $options->getInt(Option::WRITER_VERSION),
            $options->getBool(Option::DICTIONARY_ENABLED),
        );
        $requested = null;

        if ($options->has(Option::COLUMNS_ENCODINGS)) {
            $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);

            if ($columnsEncodings !== null && array_key_exists($flatPath, $columnsEncodings)) {
                // @mago-ignore analysis:mixed-assignment
                $columnEncoding = $columnsEncodings[$flatPath];

                if ($columnEncoding instanceof Encodings) {
                    $requested = $columnEncoding;
                }
            }
        }

        $encoding = $policy->encodingFor($column, $requested);

        return $encoding === Encodings::RLE_DICTIONARY
            ? new RLEDictionaryChunkBuilder(
                $column,
                $options,
                $columnCompression,
                $statistics,
                self::valueStorage($policy->fallbackEncoding($column->type()), $column),
            )
            : new FlatColumnChunkBuilder(
                $column,
                $options,
                $columnCompression,
                $statistics,
                self::valueStorage($encoding, $column),
            );
    }

    private static function valueStorage(Encodings $encoding, FlatColumn $column): ValueStorage
    {
        return match ($encoding) {
            Encodings::DELTA_BINARY_PACKED => new DeltaBinaryPackedValueStorage(),
            Encodings::DELTA_LENGTH_BYTE_ARRAY => new DeltaLengthByteArrayValueStorage(),
            Encodings::DELTA_BYTE_ARRAY => new DeltaByteArrayValueStorage(),
            Encodings::RLE => new RLEBooleanValueStorage(),
            default => $column->type() === PhysicalType::BOOLEAN ? new BooleanValueStorage() : new BufferValueStorage(),
        };
    }
}
