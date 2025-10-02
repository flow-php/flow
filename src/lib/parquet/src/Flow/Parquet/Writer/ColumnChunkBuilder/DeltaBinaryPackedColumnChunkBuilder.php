<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\{
    Data\BitWidth,
    Dremel\WriteColumnData,
    Exception\InvalidArgumentException,
    Exception\RuntimeException,
    Option,
    Options,
    ParquetFile\Data\Codec,
    Writer\ColumnChunkBuilder,
    Writer\ColumnChunkContainer,
    Writer\PageContainer,
    Writer\PageContainers,
    Writer\StatisticsCounter
};
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\{RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\{Compressions,
    Encodings
};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, PhysicalType};
use Flow\Parquet\Writer\PageBuilder\{RLEBitPackedPacker};
use Flow\Parquet\Writer\ValueStorage\{DeltaBinaryPackedValueStorage, ValueStorage};

final class DeltaBinaryPackedColumnChunkBuilder implements ColumnChunkBuilder
{
    private StatisticsCounter $chunkStatistics;

    /**
     * @var array<int>
     */
    private array $definitionLevels = [];

    private int $nonNullValuesCount = 0;

    private int $nullCount = 0;

    private PageContainers $pages;

    private StatisticsCounter $pageStatistics;

    /**
     * @var array<int>
     */
    private array $repetitionLevels = [];

    private int $rowsCount = 0;

    private readonly ValueStorage $valueStorage;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly Options $options,
        private readonly Compressions $compression,
    ) {
        $this->pages = new PageContainers();
        $this->chunkStatistics = new StatisticsCounter($this->column);
        $this->pageStatistics = new StatisticsCounter($this->column);
        $this->valueStorage = new DeltaBinaryPackedValueStorage();

        if (!in_array($this->column->type(), [PhysicalType::INT32, PhysicalType::INT64], true)) {
            throw new InvalidArgumentException('Delta encoding only supports INT32 and INT64 physical types');
        }
    }

    public function addRow(WriteColumnData $columnData) : void
    {
        $flatValues = $columnData->values($this->column->flatPath());
        $this->repetitionLevels = array_merge($this->repetitionLevels, $flatValues->repetitionLevels());
        $this->definitionLevels = array_merge($this->definitionLevels, $flatValues->definitionLevels());

        $maxDefinitionLevel = $this->column->maxDefinitionsLevel();

        foreach ($flatValues->definitionLevels() as $definitionLevel) {
            if ($definitionLevel < $maxDefinitionLevel) {
                $this->nullCount++;
            } else {
                $this->nonNullValuesCount++;
            }
        }

        $this->valueStorage->addValues($this->column, $flatValues->values());

        foreach ($flatValues->values() as $value) {
            $this->pageStatistics->add($value);
        }

        $this->rowsCount++;
    }

    public function closePage() : void
    {
        if ($this->isEmpty()) {
            return;
        }

        $codec = new Codec($this->options);

        $pageContainer = match ($writerVersion = $this->options->getInt(Option::WRITER_VERSION)) {
            1 => $this->buildDataPage($codec, $this->compression),
            2 => $this->buildDataPageV2($codec, $this->compression),
            default => throw new RuntimeException('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: ' . $writerVersion),
        };

        $this->pages->add($pageContainer);
        $this->chunkStatistics = $this->chunkStatistics->merge($this->pageStatistics);

        $this->repetitionLevels = [];
        $this->definitionLevels = [];
        $this->valueStorage->reset();
        $this->rowsCount = 0;
        $this->nullCount = 0;
        $this->nonNullValuesCount = 0;
        $this->pageStatistics = new StatisticsCounter($this->column);
    }

    public function column() : Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset) : array
    {
        $this->closePage();

        $containers = [new ColumnChunkContainer(
            $this->pages->buffer(),
            new ColumnChunk(
                type: $this->column->type(),
                codec: $this->compression,
                valuesCount: $this->pages->valuesCount(),
                fileOffset: $fileOffset,
                path: $this->column->path(),
                encodings: $this->pages->encodings(),
                totalCompressedSize: $this->pages->compressedSize(),
                totalUncompressedSize: $this->pages->uncompressedSize(),
                dictionaryPageOffset: null,
                dataPageOffset: $fileOffset,
                indexPageOffset: null,
                statistics: $this->chunkStatistics->toStatistics(),
                options: $this->options
            )
        )];

        $this->pages = new PageContainers();
        $this->chunkStatistics = new StatisticsCounter($this->column);
        $this->pageStatistics = new StatisticsCounter($this->column);
        $this->repetitionLevels = [];
        $this->definitionLevels = [];
        $this->valueStorage->reset();
        $this->rowsCount = 0;
        $this->nullCount = 0;
        $this->nonNullValuesCount = 0;

        return $containers;
    }

    /**
     * Checks if the builder has any data that needs to be written.
     * Used to prevent writing empty pages.
     */
    public function isEmpty() : bool
    {
        return $this->valueStorage->size() === 0
               && count($this->definitionLevels) === 0
               && count($this->repetitionLevels) === 0;
    }

    public function isFull() : bool
    {
        // Use the original estimation logic for compatibility with existing tests
        return $this->valueStorage->size() * ($this->column->type() === PhysicalType::INT32 ? 4 : 8) >= $this->options->get(Option::PAGE_SIZE_BYTES);
    }

    public function uncompressedSize() : int
    {
        return $this->pages->uncompressedSize() + $this->currentPageUncompressedSize();
    }

    private function buildDataPage(Codec $codec, Compressions $compression) : PageContainer
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($this->column->maxRepetitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength(BitWidth::calculate($this->column->maxRepetitionsLevel()), $this->repetitionLevels));
        }

        if ($this->column->maxDefinitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength(BitWidth::calculate($this->column->maxDefinitionsLevel()), $this->definitionLevels));
        }

        $pageWriter->append($this->valueStorage->getBuffer());

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                encoding: Encodings::DELTA_BINARY_PACKED,
                repetitionLevelEncoding: Encodings::RLE,
                definitionLevelEncoding: Encodings::RLE,
                valuesCount: \count($this->definitionLevels),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );

        return new PageContainer(
            $compressedBuffer,
            $pageHeader
        );
    }

    private function buildDataPageV2(Codec $codec, Compressions $compression) : PageContainer
    {
        $statistics = $this->pageStatistics->toStatistics();

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        if ($this->column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack(BitWidth::calculate($this->column->maxRepetitionsLevel()), $this->repetitionLevels);
            $repetitionsLength = \strlen($repetitionsBuffer);
        } else {
            $repetitionsBuffer = '';
            $repetitionsLength = 0;
        }

        if ($this->column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack(BitWidth::calculate($this->column->maxDefinitionsLevel()), $this->definitionLevels);
            $definitionsLength = \strlen($definitionsBuffer);
        } else {
            $definitionsBuffer = '';
            $definitionsLength = 0;
        }

        $encodedValues = $this->valueStorage->getBuffer();
        $compressedBuffer = $codec->compress($encodedValues, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            \strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            \strlen($encodedValues) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: \count($this->definitionLevels),
                nullsCount: $this->nullCount,
                rowsCount: $this->rowsCount,
                encoding: Encodings::DELTA_BINARY_PACKED,
                definitionsByteLength: $definitionsLength,
                repetitionsByteLength: $repetitionsLength,
                isCompressed: !($compression === Compressions::UNCOMPRESSED),
                statistics: $statistics,
            ),
            dictionaryPageHeader: null,
        );

        return new PageContainer(
            $repetitionsBuffer . $definitionsBuffer . $compressedBuffer,
            $pageHeader
        );
    }

    private function currentPageUncompressedSize() : int
    {
        return $this->valueStorage->size() + (count($this->repetitionLevels) * 4) + (count($this->definitionLevels) * 4);
    }
}
