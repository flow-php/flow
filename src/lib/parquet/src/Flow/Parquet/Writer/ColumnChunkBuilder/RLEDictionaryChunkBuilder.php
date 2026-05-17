<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\BitWidth;
use Flow\Parquet\Data\PlainValuesPacker;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\Codec;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\ColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkContainer;
use Flow\Parquet\Writer\PageBuilder\Dictionary;
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use Flow\Parquet\Writer\PageContainer;
use Flow\Parquet\Writer\PageContainers;
use Flow\Parquet\Writer\StatisticsCounter;

final class RLEDictionaryChunkBuilder implements ColumnChunkBuilder
{
    private readonly ByteOrder $byteOrder;

    private StatisticsCounter $chunkStatistics;

    /**
     * @var array<int>
     */
    private array $definitionLevels = [];

    private ?Dictionary $dictionary = null;

    private int $nullCount = 0;

    private PageContainers $pages;

    private StatisticsCounter $pageStatistics;

    /**
     * @var array<null|bool|float|int|object|string>
     */
    private array $pageValues = [];

    /**
     * @var array<int>
     */
    private array $repetitionLevels = [];

    private int $rowsCount = 0;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly Options $options,
        private readonly Compressions $compression,
    ) {
        $this->pages = new PageContainers();
        $this->chunkStatistics = new StatisticsCounter($this->column);
        $this->pageStatistics = new StatisticsCounter($this->column);
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
    }

    public function addColumn(WriteFlatColumnValues $columnValues): void
    {
        array_push($this->repetitionLevels, ...$columnValues->repetitionLevels());

        $defLevels = $columnValues->definitionLevels();
        array_push($this->definitionLevels, ...$defLevels);

        $maxDefinitionLevel = $this->column->maxDefinitionsLevel();

        $nullsInBatch = 0;

        foreach ($defLevels as $definitionLevel) {
            if ($definitionLevel < $maxDefinitionLevel) {
                $this->nullCount++;
                $nullsInBatch++;
            }
        }

        array_push($this->pageValues, ...$columnValues->values());
        $this->pageStatistics->addBatch($columnValues->values());

        if ($nullsInBatch > 0) {
            $this->pageStatistics->addNulls($nullsInBatch);
        }

        $this->rowsCount += $columnValues->rowsCount();
    }

    public function closePage(): void
    {
        if ($this->isEmpty()) {
            return;
        }

        $codec = new Codec($this->options);

        if (\count($this->pageValues) > 0) {
            $flatColumnValues = new WriteFlatColumnValues(
                $this->column,
                $this->repetitionLevels,
                $this->definitionLevels,
                $this->pageValues,
            );

            $this->dictionary = (new DictionaryBuilder())->build($this->column, $flatColumnValues);

            if (!$this->pages->dictionaryPageContainer()) {
                $dictionaryPageContainer = $this->buildDictionaryPage($codec, $this->compression);
                $this->pages->add($dictionaryPageContainer);
            }
        }

        $pageContainer = match ($writerVersion = $this->options->getInt(Option::WRITER_VERSION)) {
            1 => $this->buildDataPage($codec, $this->compression),
            2 => $this->buildDataPageV2($codec, $this->compression),
            default => throw new RuntimeException(
                'Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: '
                . $writerVersion,
            ),
        };

        $this->pages->add($pageContainer);
        $this->chunkStatistics = $this->chunkStatistics->merge($this->pageStatistics);

        $this->repetitionLevels = [];
        $this->definitionLevels = [];
        $this->pageValues = [];
        $this->rowsCount = 0;
        $this->nullCount = 0;
        $this->pageStatistics = new StatisticsCounter($this->column);
    }

    public function column(): Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset): array
    {
        $this->closePage();

        $dictionaryPageContainer = $this->pages->dictionaryPageContainer();

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
                dictionaryPageOffset: $dictionaryPageContainer !== null ? $fileOffset : null,
                dataPageOffset: $dictionaryPageContainer !== null
                    ? $fileOffset + $dictionaryPageContainer->totalCompressedSize()
                    : $fileOffset,
                indexPageOffset: null,
                statistics: $this->chunkStatistics->toStatistics(),
            ),
        )];

        $this->pages = new PageContainers();
        $this->chunkStatistics = new StatisticsCounter($this->column);
        $this->pageStatistics = new StatisticsCounter($this->column);
        $this->definitionLevels = [];
        $this->repetitionLevels = [];
        $this->pageValues = [];
        $this->dictionary = null;
        $this->rowsCount = 0;
        $this->nullCount = 0;

        return $containers;
    }

    public function isEmpty(): bool
    {
        return (
            count($this->pageValues) === 0
            && count($this->definitionLevels) === 0
            && count($this->repetitionLevels) === 0
        );
    }

    public function isFull(): bool
    {
        return (\count($this->pageValues) * 4) >= $this->options->getInt(Option::PAGE_SIZE_BYTES);
    }

    public function uncompressedSize(): int
    {
        return $this->pages->uncompressedSize() + $this->currentPageUncompressedSize();
    }

    private function buildDataPage(Codec $codec, Compressions $compression): PageContainer
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $packer = new RLEBitPackedPacker($rleBitPackedHybrid, $this->byteOrder);

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($this->column->maxRepetitionsLevel() > 0) {
            $pageWriter->append($packer->packWithLength(
                BitWidth::calculate($this->column->maxRepetitionsLevel()),
                $this->repetitionLevels,
            ));
        }

        if ($this->column->maxDefinitionsLevel() > 0) {
            $pageWriter->append($packer->packWithLength(
                BitWidth::calculate($this->column->maxDefinitionsLevel()),
                $this->definitionLevels,
            ));
        }

        if ($this->dictionary && \count($this->dictionary->indices) > 0) {
            $bitWidth = BitWidth::fromArray($this->dictionary->indices);
            $pageWriter->append($packer->packWithBitWidth($bitWidth, $this->dictionary->indices));
        }

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                encoding: Encodings::RLE_DICTIONARY,
                repetitionLevelEncoding: Encodings::RLE,
                definitionLevelEncoding: Encodings::RLE,
                valuesCount: \count($this->definitionLevels),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );

        return new PageContainer($compressedBuffer, $pageHeader);
    }

    private function buildDataPageV2(Codec $codec, Compressions $compression): PageContainer
    {
        $statistics = $this->pageStatistics->toStatistics();

        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $packer = new RLEBitPackedPacker($rleBitPackedHybrid, $this->byteOrder);

        if ($this->column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = $packer->pack(
                BitWidth::calculate($this->column->maxRepetitionsLevel()),
                $this->repetitionLevels,
            );
            $repetitionsLength = \strlen($repetitionsBuffer);
        } else {
            $repetitionsBuffer = '';
            $repetitionsLength = 0;
        }

        if ($this->column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = $packer->pack(
                BitWidth::calculate($this->column->maxDefinitionsLevel()),
                $this->definitionLevels,
            );
            $definitionsLength = \strlen($definitionsBuffer);
        } else {
            $definitionsBuffer = '';
            $definitionsLength = 0;
        }

        $indicesBuffer = '';

        if ($this->dictionary && \count($this->dictionary->indices) > 0) {
            $bitWidth = BitWidth::fromArray($this->dictionary->indices);
            $indicesBuffer = $packer->packWithBitWidth($bitWidth, $this->dictionary->indices);
        }

        $compressedBuffer = $codec->compress($indicesBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            \strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            \strlen($indicesBuffer) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: \count($this->definitionLevels),
                nullsCount: $this->nullCount,
                rowsCount: $this->rowsCount,
                encoding: Encodings::RLE_DICTIONARY,
                definitionsByteLength: $definitionsLength,
                repetitionsByteLength: $repetitionsLength,
                isCompressed: !($compression === Compressions::UNCOMPRESSED),
                statistics: $statistics,
            ),
            dictionaryPageHeader: null,
        );

        return new PageContainer($repetitionsBuffer . $definitionsBuffer . $compressedBuffer, $pageHeader);
    }

    private function buildDictionaryPage(Codec $codec, Compressions $compression): PageContainer
    {
        $dictionary = $this->dictionary;

        if ($dictionary === null) {
            throw new RuntimeException('Cannot build dictionary page without dictionary');
        }

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        (new PlainValuesPacker($pageWriter, $this->byteOrder))->packValues($this->column, $dictionary->dictionary);

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(Encodings::PLAIN, \count($dictionary->dictionary)),
        );

        return new PageContainer($compressedBuffer, $pageHeader);
    }

    private function currentPageUncompressedSize(): int
    {
        return (
            (count($this->pageValues) * 4) + (count($this->repetitionLevels) * 4) + (count($this->definitionLevels) * 4)
        );
    }
}
