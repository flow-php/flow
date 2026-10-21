<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\DeltaBinaryPackedDecoder;
use Flow\Parquet\Data\DeltaByteArrayDecoder;
use Flow\Parquet\Data\DeltaLengthByteArrayDecoder;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Generator;

use function array_key_exists;
use function array_map;
use function in_array;
use function ord;
use function sprintf;
use function strlen;

final readonly class PageValuesDecoder
{
    public function __construct(
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private RLEBitPackedHybrid $rleBitPackedHybrid = new RLEBitPackedHybrid(),
        private DeltaBinaryPackedDecoder $int32Deltas = new DeltaBinaryPackedDecoder(physicalType: PhysicalType::INT32),
        private DeltaBinaryPackedDecoder $int64Deltas = new DeltaBinaryPackedDecoder(physicalType: PhysicalType::INT64),
        private DeltaLengthByteArrayDecoder $deltaLengthByteArray = new DeltaLengthByteArrayDecoder(),
        private DeltaByteArrayDecoder $deltaByteArray = new DeltaByteArrayDecoder(),
    ) {}

    public function decode(
        BinaryBufferReader $reader,
        FlatColumn $column,
        Encodings $encoding,
        int $valuesCount,
        ?Dictionary $dictionary = null,
    ): Generator {
        yield from match ($encoding) {
            Encodings::PLAIN => (new PlainValueUnpacker($reader, $this->byteOrder))->unpack($column, $valuesCount),
            Encodings::RLE => $this->rleBooleans($reader, $column, $valuesCount),
            Encodings::DELTA_BINARY_PACKED => $this->deltaBinaryPacked($reader, $column, $valuesCount),
            Encodings::DELTA_LENGTH_BYTE_ARRAY => $this->deltaLengthByteArray($reader, $column, $valuesCount),
            Encodings::DELTA_BYTE_ARRAY => $this->deltaByteArray($reader, $column, $valuesCount),
            Encodings::RLE_DICTIONARY, Encodings::PLAIN_DICTIONARY => $this->dictionary(
                $reader,
                $valuesCount,
                $dictionary,
            ),
            default => throw new RuntimeException('Encoding ' . $encoding->name . ' not supported'),
        };
    }

    /**
     * @return array<int>
     */
    private function deltaBinaryPacked(BinaryBufferReader $reader, FlatColumn $column, int $valuesCount): array
    {
        if (!in_array($column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
            throw new RuntimeException('Delta encoding only supports INT32 and INT64 physical types');
        }

        return $column->type() === PhysicalType::INT32
            ? $this->int32Deltas->decode($reader, $valuesCount)
            : $this->int64Deltas->decode($reader, $valuesCount);
    }

    /**
     * @return list<string>
     */
    private function deltaByteArray(BinaryBufferReader $reader, FlatColumn $column, int $valuesCount): array
    {
        if (!in_array($column->type(), [PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY], true)) {
            throw new RuntimeException(
                'DELTA_BYTE_ARRAY encoding only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY physical types',
            );
        }

        $values = $this->deltaByteArray->decode($reader, $valuesCount);

        if ($column->type() === PhysicalType::FIXED_LEN_BYTE_ARRAY) {
            foreach ($values as $index => $value) {
                if (strlen($value) !== $column->typeLength()) {
                    throw new RuntimeException(sprintf(
                        "DELTA_BYTE_ARRAY value %d of FIXED_LEN_BYTE_ARRAY column '%s' has %d bytes, expected %d",
                        $index,
                        $column->flatPath(),
                        strlen($value),
                        (int) $column->typeLength(),
                    ));
                }
            }
        }

        return $values;
    }

    /**
     * @return list<string>
     */
    private function deltaLengthByteArray(BinaryBufferReader $reader, FlatColumn $column, int $valuesCount): array
    {
        if ($column->type() !== PhysicalType::BYTE_ARRAY) {
            throw new RuntimeException('DELTA_LENGTH_BYTE_ARRAY encoding only supports BYTE_ARRAY physical type');
        }

        return $this->deltaLengthByteArray->decode($reader, $valuesCount);
    }

    /**
     * @return array<mixed>
     */
    private function dictionary(BinaryBufferReader $reader, int $valuesCount, ?Dictionary $dictionary): array
    {
        if ($valuesCount === 0) {
            return [];
        }

        $bitWidth = ord($reader->readBytes(1));

        return array_map(
            static fn(int $index): mixed => $dictionary !== null && array_key_exists($index, $dictionary->values)
                ? $dictionary->values[$index]
                : null,
            $this->rleBitPackedHybrid->decodeHybrid($reader, $bitWidth, $valuesCount),
        );
    }

    /**
     * @return array<bool>
     */
    private function rleBooleans(BinaryBufferReader $reader, FlatColumn $column, int $valuesCount): array
    {
        if ($column->type() !== PhysicalType::BOOLEAN) {
            throw new RuntimeException('Encoding RLE not supported');
        }

        // RLE data values carry their 4-byte length first
        $reader->seekBytes(4);

        return array_map(
            static fn(int $value): bool => $value === 1,
            $this->rleBitPackedHybrid->decodeHybrid($reader, 1, $valuesCount),
        );
    }
}
