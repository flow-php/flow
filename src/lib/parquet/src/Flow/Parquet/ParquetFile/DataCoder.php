<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\{BitWidth, PlainValueUnpacker, RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader};
use Flow\Parquet\ParquetFile\Page\{Dictionary};
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\{ByteOrder, Options, ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues};

final class DataCoder
{
    public function __construct(
        private readonly Options $options,
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {
    }

    public function decodeData(
        string $buffer,
        FlatColumn $column,
        DataPageHeader $pageHeader,
        ?Dictionary $dictionary = null,
    ) : FlatColumnValues {

        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $reader->readInts32(1); // read length of encoded data
            $repetitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxRepetitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $repetitionLevels = \array_fill(0, $pageHeader->valuesCount(), 0);
        }

        if ($column->maxDefinitionsLevel()) {
            $reader->readInts32(1); // read length of encoded data
            $definitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxDefinitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $definitionLevels = \array_fill(0, $pageHeader->valuesCount(), $column->maxDefinitionsLevel());
        }

        $nonEmptyValuesCount = $this->countValues($definitionLevels, $column);

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new FlatColumnValues(
                $column,
                $repetitionLevels,
                $definitionLevels,
                (new PlainValueUnpacker($reader, $this->options))->unpack($column, $nonEmptyValuesCount)
            );
        }

        if ($pageHeader->encoding() === Encodings::RLE_DICTIONARY || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY) {
            if ($nonEmptyValuesCount) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid(
                    $reader,
                    $RLEBitPackedHybrid,
                    $bitWidth,
                    $nonEmptyValuesCount
                );

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = $dictionary && \array_key_exists($index, $dictionary->values) ? $dictionary->values[$index] : null;
                }
            } else {
                $values = [];
            }

            return new FlatColumnValues($column, $repetitionLevels, $definitionLevels, $values);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDataV2(
        string $buffer,
        FlatColumn $column,
        DataPageHeaderV2 $pageHeader,
        ?Dictionary $dictionary = null,
    ) : FlatColumnValues {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        $RLEBitPackedHybrid = new RLEBitPackedHybrid();

        if ($column->maxRepetitionsLevel()) {
            $repetitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxRepetitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $repetitionLevels = \array_fill(0, $pageHeader->valuesCount(), 0);
        }

        if ($column->maxDefinitionsLevel()) {
            $definitionLevels = $this->readRLEBitPackedHybrid(
                $reader,
                $RLEBitPackedHybrid,
                BitWidth::calculate($column->maxDefinitionsLevel()),
                $pageHeader->valuesCount(),
            );
        } else {
            $definitionLevels = \array_fill(0, $pageHeader->valuesCount(), $column->maxDefinitionsLevel());
        }

        $nonEmptyValuesCount = $this->countValues($definitionLevels, $column);

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new FlatColumnValues(
                $column,
                $repetitionLevels,
                $definitionLevels,
                (new PlainValueUnpacker($reader, $this->options))->unpack($column, $nonEmptyValuesCount)
            );
        }

        if ($pageHeader->encoding() === Encodings::RLE_DICTIONARY || $pageHeader->encoding() === Encodings::PLAIN_DICTIONARY) {
            if (\count($definitionLevels)) {
                // while reading indices, there is no length at the beginning since length is simply a remaining length of the buffer
                // however we need to know bitWidth which is the first value in the buffer after definitions
                $bitWidth = $reader->readBytes(1)->toInt();
                /** @var array<int> $indices */
                $indices = $this->readRLEBitPackedHybrid(
                    $reader,
                    $RLEBitPackedHybrid,
                    $bitWidth,
                    $nonEmptyValuesCount,
                );

                /** @var array<mixed> $values */
                $values = [];

                foreach ($indices as $index) {
                    $values[] = $dictionary?->values[$index];
                }
            } else {
                $values = [];
            }

            return new FlatColumnValues($column, $repetitionLevels, $definitionLevels, $values);
        }

        throw new RuntimeException('Encoding ' . $pageHeader->encoding()->name . ' not supported');
    }

    public function decodeDictionary(
        string $buffer,
        FlatColumn $column,
        DictionaryPageHeader $pageHeader,
    ) : Dictionary {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        return new Dictionary(
            (new PlainValueUnpacker($reader, $this->options))->unpack($column, $pageHeader->valuesCount())
        );
    }

    private function countValues(array $definitions, FlatColumn $column) : int
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

    private function readRLEBitPackedHybrid(BinaryBufferReader $reader, RLEBitPackedHybrid $RLEBitPackedHybrid, int $bitWidth, int $expectedValuesCount) : array
    {
        return $RLEBitPackedHybrid->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
    }
}
