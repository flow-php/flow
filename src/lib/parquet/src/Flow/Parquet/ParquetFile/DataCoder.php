<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class DataCoder
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN
    ) {
    }

    public function decodeData(
        string $buffer,
        Encodings $encoding,
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
        int $expectedValuesCount,
        int $maxRepetitionsLevel,
        int $maxDefinitionsLevel,
        ?int $typeLength = null,
        ?Dictionary $dictionary = null
    ) : ColumnData {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($maxRepetitionsLevel) {
            $reader->readInts32(1); // read length of encoded data
            $repetitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxRepetitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $repetitions = [];
        }

        if ($maxDefinitionsLevel) {
            $reader->readInts32(1); // read length of encoded data
            $definitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxDefinitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $definitions = [];
        }

        $nullsCount = \count($definitions) ? \count(\array_filter($definitions, static fn ($definition) => $definition !== $maxDefinitionsLevel)) : 0;

        if ($encoding === Encodings::PLAIN) {
            return new ColumnData(
                $physicalType,
                $logicalType,
                $repetitions,
                $definitions,
                $this->readPlainValues(
                    $physicalType,
                    $reader,
                    $expectedValuesCount - $nullsCount,
                    $logicalType,
                    $typeLength
                )
            );
        }

        if ($encoding === Encodings::RLE_DICTIONARY || $encoding === Encodings::PLAIN_DICTIONARY) {
            if (\count($definitions)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid(
                    $reader,
                    $RLEBitPackedHybrid,
                    $bitWidth,
                    $expectedValuesCount - $nullsCount,
                );

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = $dictionary?->values[$index];
                }
            } else {
                $values = [];
            }

            return new ColumnData($physicalType, $logicalType, $repetitions, $definitions, $values);
        }

        throw new RuntimeException('Encoding ' . $encoding->name . ' not supported');
    }

    public function decodeDataV2(
        string $buffer,
        Encodings $encoding,
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
        int $expectedValuesCount,
        int $maxRepetitionsLevel,
        int $maxDefinitionsLevel,
        ?int $typeLength = null,
        ?Dictionary $dictionary = null
    ) : ColumnData {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($maxRepetitionsLevel) {
            $repetitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxRepetitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $repetitions = [];
        }

        if ($maxDefinitionsLevel) {
            $definitions = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($maxDefinitionsLevel),
                $expectedValuesCount,
            );
        } else {
            $definitions = [];
        }

        $nullsCount = \count($definitions) ? \count(\array_filter($definitions, static fn ($definition) => $definition !== $maxDefinitionsLevel)) : 0;

        if ($encoding === Encodings::PLAIN) {
            return new ColumnData(
                $physicalType,
                $logicalType,
                $repetitions,
                $definitions,
                $this->readPlainValues(
                    $physicalType,
                    $reader,
                    $expectedValuesCount - $nullsCount,
                    $logicalType,
                    $typeLength
                )
            );
        }

        if ($encoding === Encodings::RLE_DICTIONARY || $encoding === Encodings::PLAIN_DICTIONARY) {
            if (\count($definitions)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid(
                    $reader,
                    $RLEBitPackedHybrid,
                    $bitWidth,
                    $expectedValuesCount - $nullsCount,
                );

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = $dictionary?->values[$index];
                }
            } else {
                $values = [];
            }

            return new ColumnData($physicalType, $logicalType, $repetitions, $definitions, $values);
        }

        throw new RuntimeException('Encoding ' . $encoding->name . ' not supported');
    }

    public function decodeDictionary(
        string $buffer,
        PhysicalType $physicalType,
        ?LogicalType $logicalType,
        Encodings $encoding,
        int $expectedValuesCount,
        ?int $typeLength = null
    ) : Dictionary {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        return new Dictionary(
            $this->readPlainValues($physicalType, $reader, $expectedValuesCount, $logicalType, $typeLength)
        );
    }

    /**
     * @psalm-suppress PossiblyNullReference
     */
    private function readPlainValues(PhysicalType $physicalType, BinaryBufferReader $reader, int $total, ?LogicalType $logicalType, ?int $typeLength) : array
    {
        /** @psalm-suppress PossiblyNullArgument */
        return match ($physicalType) {
            PhysicalType::INT32 => $reader->readInts32($total),
            PhysicalType::INT64 => $reader->readInts64($total),
            PhysicalType::INT96 => $reader->readInts96($total),
            PhysicalType::FLOAT => $reader->readFloats($total),
            PhysicalType::DOUBLE => $reader->readDoubles($total),
            PhysicalType::BYTE_ARRAY => match ($logicalType?->name()) {
                LogicalType::STRING => $reader->readStrings($total),
                default => $reader->readByteArrays($total)
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => match ($logicalType?->name()) {
                /** @phpstan-ignore-next-line */
                LogicalType::DECIMAL => $reader->readDecimals($total, $typeLength, $logicalType->decimalData()->precision(), $logicalType->decimalData()->scale()),
                default => throw new RuntimeException('Unsupported logical type ' . ($logicalType?->name() ?: 'null') . ' for FIXED_LEN_BYTE_ARRAY'),
            },
            PhysicalType::BOOLEAN => $reader->readBooleans($total),
        };
    }

    private function readRLEBitPackedHybrid(BinaryBufferReader $reader, RLEBitPackedHybrid $RLEBitPackedHybrid, int $bitWidth, int $expectedValuesCount) : array
    {
        return $RLEBitPackedHybrid->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
    }
}
