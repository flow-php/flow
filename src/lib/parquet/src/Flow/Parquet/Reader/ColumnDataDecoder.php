<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\BitWidth;
use Flow\Parquet\Data\DeltaBinaryPackedDecoder;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_fill;
use function array_key_exists;
use function count;
use function Flow\Parquet\empty_generator;
use function iterator_to_array;
use function ord;

final readonly class ColumnDataDecoder
{
    public function __construct(
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {}

    public function decodeData(
        string $buffer,
        FlatColumn $column,
        DataPageHeader $pageHeader,
        ?Dictionary $dictionary = null,
    ): ReadFlatColumnValues {
        $reader = new BinaryBufferReader($buffer);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $reader->seekBytes(4);
            $repetitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxRepetitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $repetitionLevels = array_fill(0, max(0, $pageHeader->valuesCount()), 0);
        }

        if ($column->maxDefinitionsLevel()) {
            $reader->seekBytes(4);
            $definitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxDefinitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $definitionLevels = array_fill(0, max(0, $pageHeader->valuesCount()), $column->maxDefinitionsLevel());
        }

        $nonEmptyValuesCount = $this->countValues($definitionLevels, $column);

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new ReadFlatColumnValues(
                $column,
                (new PlainValueUnpacker($reader, $this->byteOrder))->unpack($column, $nonEmptyValuesCount),
                $repetitionLevels,
                $definitionLevels,
            );
        }

        if ($pageHeader->encoding() === Encodings::DELTA_BINARY_PACKED) {
            if (!in_array($column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
                throw new RuntimeException('Delta encoding only supports INT32 and INT64 physical types');
            }

            if ($nonEmptyValuesCount === 0) {
                return new ReadFlatColumnValues($column, empty_generator(), $repetitionLevels, $definitionLevels);
            }

            $remainingData = $reader->readBytes($reader->remainingLength()->bytes());

            $decoder = new DeltaBinaryPackedDecoder();
            $values = $decoder->decode($remainingData, $nonEmptyValuesCount);

            $valuesGenerator = static function () use ($values) {
                foreach ($values as $value) {
                    yield $value;
                }
            };

            return new ReadFlatColumnValues($column, $valuesGenerator(), $repetitionLevels, $definitionLevels);
        }

        if (
            $pageHeader->encoding() === Encodings::RLE_DICTIONARY
            || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY
        ) {
            if ($nonEmptyValuesCount) {
                $bitWidth = ord($reader->readBytes(1));

                $indices = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, $bitWidth, $nonEmptyValuesCount);

                $valuesGenerator = static function () use ($indices, $dictionary) {
                    foreach ($indices as $index) {
                        yield $dictionary && array_key_exists($index, $dictionary->values)
                            ? $dictionary->values[$index]
                            : null;
                    }
                };

                return new ReadFlatColumnValues($column, $valuesGenerator(), $repetitionLevels, $definitionLevels);
            }

            return new ReadFlatColumnValues($column, empty_generator(), $repetitionLevels, $definitionLevels);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDataV2(
        string $buffer,
        FlatColumn $column,
        DataPageHeaderV2 $pageHeader,
        ?Dictionary $dictionary = null,
    ): ReadFlatColumnValues {
        $reader = new BinaryBufferReader($buffer);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $repetitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxRepetitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $repetitionLevels = array_fill(0, max(0, $pageHeader->valuesCount()), 0);
        }

        if ($column->maxDefinitionsLevel()) {
            $definitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxDefinitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $definitionLevels = array_fill(0, max(0, $pageHeader->valuesCount()), $column->maxDefinitionsLevel());
        }

        $nonEmptyValuesCount = $this->countValues($definitionLevels, $column);

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new ReadFlatColumnValues(
                $column,
                (new PlainValueUnpacker($reader, $this->byteOrder))->unpack($column, $nonEmptyValuesCount),
                $repetitionLevels,
                $definitionLevels,
            );
        }

        if ($pageHeader->encoding() === Encodings::DELTA_BINARY_PACKED) {
            if (!in_array($column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
                throw new RuntimeException('Delta encoding only supports INT32 and INT64 physical types');
            }

            if ($nonEmptyValuesCount === 0) {
                return new ReadFlatColumnValues($column, empty_generator(), $repetitionLevels, $definitionLevels);
            }

            $remainingData = $reader->readBytes($reader->remainingLength()->bytes());

            $decoder = new DeltaBinaryPackedDecoder();
            $values = $decoder->decode($remainingData, $nonEmptyValuesCount);

            $valuesGenerator = static function () use ($values) {
                foreach ($values as $value) {
                    yield $value;
                }
            };

            return new ReadFlatColumnValues($column, $valuesGenerator(), $repetitionLevels, $definitionLevels);
        }

        if (
            $pageHeader->encoding() === Encodings::RLE_DICTIONARY
            || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY
        ) {
            if (count($definitionLevels)) {
                $bitWidth = ord($reader->readBytes(1));

                $indices = $this->readRLEBitPackedHybrid($reader, $RLEBitPackedHybrid, $bitWidth, $nonEmptyValuesCount);

                $valuesGenerator = static function () use ($indices, $dictionary) {
                    foreach ($indices as $index) {
                        yield $dictionary?->values[$index];
                    }
                };

                return new ReadFlatColumnValues($column, $valuesGenerator(), $repetitionLevels, $definitionLevels);
            }

            return new ReadFlatColumnValues($column, empty_generator(), $repetitionLevels, $definitionLevels);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDictionary(string $buffer, FlatColumn $column, DictionaryPageHeader $pageHeader): Dictionary
    {
        $reader = new BinaryBufferReader($buffer);

        return new Dictionary(iterator_to_array((new PlainValueUnpacker($reader, $this->byteOrder))->unpack(
            $column,
            $pageHeader->valuesCount(),
        )));
    }

    /**
     * @param array<int> $definitions
     */
    private function countValues(array $definitions, FlatColumn $column): int
    {
        $maxDefinitionLevel = $column->maxDefinitionsLevel();
        $valuesCount = 0;

        foreach ($definitions as $definition) {
            if ($definition === $maxDefinitionLevel) {
                $valuesCount++;
            }
        }

        return $valuesCount;
    }

    /**
     * @return array<int>
     */
    private function readRLEBitPackedHybrid(
        BinaryBufferReader $reader,
        RLEBitPackedHybrid $RLEBitPackedHybrid,
        int $bitWidth,
        int $expectedValuesCount,
    ): array {
        return $RLEBitPackedHybrid->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
    }
}
