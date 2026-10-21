<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\BitWidth;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_fill;
use function iterator_to_array;

final readonly class ColumnDataDecoder
{
    private PageValuesDecoder $pageValues;

    public function __construct(
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {
        $this->pageValues = new PageValuesDecoder($this->byteOrder);
    }

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

        return new ReadFlatColumnValues(
            $column,
            $this->pageValues->decode(
                $reader,
                $column,
                $pageHeader->encoding(),
                $this->countValues($definitionLevels, $column),
                $dictionary,
            ),
            $repetitionLevels,
            $definitionLevels,
        );
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

        return new ReadFlatColumnValues(
            $column,
            $this->pageValues->decode(
                $reader,
                $column,
                $pageHeader->encoding(),
                $this->countValues($definitionLevels, $column),
                $dictionary,
            ),
            $repetitionLevels,
            $definitionLevels,
        );
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
