<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use function Flow\ETL\Adapter\Parquet\empty_generator;
use Flow\Parquet\{
    ByteOrder,
    Dremel\ColumnData\ReadFlatColumnValues,
    Options,
    ParquetFile\Encodings
};
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\{BitWidth, PlainValueUnpacker, RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\Page\{Dictionary};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader};
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class ColumnDataDecoder
{
    public function __construct(
        private Options $options,
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {
    }

    public function decodeData(
        string $buffer,
        FlatColumn $column,
        DataPageHeader $pageHeader,
        ?Dictionary $dictionary = null,
    ) : ReadFlatColumnValues {

        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

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
            $repetitionLevels = \array_fill(0, $pageHeader->valuesCount(), 0);
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
            $definitionLevels = \array_fill(0, $pageHeader->valuesCount(), $column->maxDefinitionsLevel());
        }

        $nonEmptyValuesCount = $this->countValues($definitionLevels, $column);

        if ($pageHeader->encoding() === Encodings::PLAIN) {
            return new ReadFlatColumnValues(
                $column,
                (new PlainValueUnpacker($reader, $this->options))->unpack($column, $nonEmptyValuesCount),
                $repetitionLevels,
                $definitionLevels
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

                $valuesGenerator = function () use ($indices, $dictionary) {
                    foreach ($indices as $index) {
                        yield $dictionary && \array_key_exists($index, $dictionary->values) ? $dictionary->values[$index] : null;
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
    ) : ReadFlatColumnValues {
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
            return new ReadFlatColumnValues(
                $column,
                (new PlainValueUnpacker($reader, $this->options))->unpack($column, $nonEmptyValuesCount),
                $repetitionLevels,
                $definitionLevels
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

                $valuesGenerator = function () use ($indices, $dictionary) {
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

    public function decodeDictionary(
        string $buffer,
        FlatColumn $column,
        DictionaryPageHeader $pageHeader,
    ) : Dictionary {
        $reader = new BinaryBufferReader($buffer, $this->byteOrder);

        return new Dictionary(
            \iterator_to_array((new PlainValueUnpacker($reader, $this->options))->unpack($column, $pageHeader->valuesCount()))
        );
    }

    /**
     * @param array<int> $definitions
     */
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

    /**
     * @return array<int>
     */
    private function readRLEBitPackedHybrid(BinaryBufferReader $reader, RLEBitPackedHybrid $RLEBitPackedHybrid, int $bitWidth, int $expectedValuesCount) : array
    {
        return $RLEBitPackedHybrid->decodeHybrid($reader, $bitWidth, $expectedValuesCount);
    }
}
