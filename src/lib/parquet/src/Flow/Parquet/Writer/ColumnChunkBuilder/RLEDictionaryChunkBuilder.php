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
use Flow\Parquet\Writer\PageIndexBuilder;
use Flow\Parquet\Writer\StatisticsCounter;
use Flow\Parquet\Writer\StatisticsOptions;

use function array_slice;
use function count;
use function strlen;

final class RLEDictionaryChunkBuilder implements ColumnChunkBuilder
{
    private readonly ByteOrder $byteOrder;

    private StatisticsCounter $chunkStatistics;

    /**
     * @var array<int>
     */
    private array $definitionLevels = [];

    /**
     * The chunk's dictionary: every page's indices point into it, its page is written first at flush.
     */
    private Dictionary $dictionary;

    /**
     * The dictionary's PLAIN encoded size, compared to Option::DICTIONARY_PAGE_SIZE.
     */
    private int $dictionaryBytes = 0;

    private int $nullCount = 0;

    private PageContainers $pages;

    private StatisticsCounter $pageStatistics;

    private PageIndexBuilder $pageIndex;

    /**
     * @var array<null|bool|float|int|object|string>
     */
    private array $pageValues = [];

    /**
     * @var array<int>
     */
    private array $repetitionLevels = [];

    private int $rowsCount = 0;

    /**
     * Set once a page would grow the dictionary past Option::DICTIONARY_PAGE_SIZE: that page and the rest of the chunk
     * are PLAIN, as arrow-rs falls back.
     */
    private bool $plain = false;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly Options $options,
        private readonly Compressions $compression,
        private readonly StatisticsOptions $statistics,
    ) {
        $this->pageIndex = $this->statistics->pageIndex($this->column);
        $this->pages = new PageContainers();
        $this->chunkStatistics = $this->statistics->counter($this->column);
        $this->pageStatistics = $this->statistics->counter($this->column);
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
        $this->dictionary = new Dictionary([], []);
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
        $indices = $this->plain ? null : $this->dictionaryIndices();

        $pageContainer = match ($writerVersion = $this->options->getInt(Option::WRITER_VERSION)) {
            1 => $this->buildDataPage($codec, $this->compression, $indices),
            2 => $this->buildDataPageV2($codec, $this->compression, $indices),
            default => throw new RuntimeException(
                'Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: '
                . $writerVersion,
            ),
        };

        $this->pages->add($pageContainer);
        $this->pageIndex->addPage($this->pageStatistics, $this->rowsCount);
        $this->chunkStatistics = $this->chunkStatistics->merge($this->pageStatistics);

        $this->repetitionLevels = [];
        $this->definitionLevels = [];
        $this->pageValues = [];
        $this->rowsCount = 0;
        $this->nullCount = 0;
        $this->pageStatistics = $this->statistics->counter($this->column);
    }

    public function column(): Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset): array
    {
        $this->closePage();

        if ($this->dictionary->dictionary !== []) {
            $this->pages->add($this->buildDictionaryPage(new Codec($this->options), $this->compression));
        }

        $dictionaryPageContainer = $this->pages->dictionaryPageContainer();

        $dataPageOffset = $dictionaryPageContainer !== null
            ? $fileOffset + $dictionaryPageContainer->totalCompressedSize()
            : $fileOffset;
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
                dataPageOffset: $dataPageOffset,
                indexPageOffset: null,
                statistics: $this->statistics->chunk($this->chunkStatistics),
            ),
            $this->pageIndex->columnIndex(),
            $this->pageIndex->offsetIndex($dataPageOffset, $this->pages),
        )];

        $this->pages = new PageContainers();
        $this->pageIndex = $this->statistics->pageIndex($this->column);
        $this->chunkStatistics = $this->statistics->counter($this->column);
        $this->pageStatistics = $this->statistics->counter($this->column);
        $this->definitionLevels = [];
        $this->repetitionLevels = [];
        $this->pageValues = [];
        $this->dictionary = new Dictionary([], []);
        $this->dictionaryBytes = 0;
        $this->plain = false;
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
        return (count($this->pageValues) * 4) >= $this->options->getInt(Option::PAGE_SIZE_BYTES);
    }

    public function uncompressedSize(): int
    {
        return $this->pages->uncompressedSize() + $this->currentPageUncompressedSize();
    }

    /**
     * @param null|array<int, int> $indices the page's dictionary indices, null for a PLAIN page
     */
    private function buildDataPage(Codec $codec, Compressions $compression, ?array $indices): PageContainer
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

        $pageWriter->append($this->values($packer, $indices));

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            strlen($compressedBuffer),
            strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                encoding: $indices === null ? Encodings::PLAIN : Encodings::RLE_DICTIONARY,
                repetitionLevelEncoding: Encodings::RLE,
                definitionLevelEncoding: Encodings::RLE,
                valuesCount: count($this->definitionLevels),
                statistics: $this->statistics->pageHeader($this->pageStatistics),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );

        return new PageContainer($compressedBuffer, $pageHeader);
    }

    /**
     * @param null|array<int, int> $indices the page's dictionary indices, null for a PLAIN page
     */
    private function buildDataPageV2(Codec $codec, Compressions $compression, ?array $indices): PageContainer
    {
        $statistics = $this->statistics->pageHeader($this->pageStatistics);

        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $packer = new RLEBitPackedPacker($rleBitPackedHybrid, $this->byteOrder);

        if ($this->column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = $packer->pack(
                BitWidth::calculate($this->column->maxRepetitionsLevel()),
                $this->repetitionLevels,
            );
            $repetitionsLength = strlen($repetitionsBuffer);
        } else {
            $repetitionsBuffer = '';
            $repetitionsLength = 0;
        }

        if ($this->column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = $packer->pack(
                BitWidth::calculate($this->column->maxDefinitionsLevel()),
                $this->definitionLevels,
            );
            $definitionsLength = strlen($definitionsBuffer);
        } else {
            $definitionsBuffer = '';
            $definitionsLength = 0;
        }

        $valuesBuffer = $this->values($packer, $indices);
        $compressedBuffer = $codec->compress($valuesBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            strlen($valuesBuffer) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: count($this->definitionLevels),
                nullsCount: $this->nullCount,
                rowsCount: $this->rowsCount,
                encoding: $indices === null ? Encodings::PLAIN : Encodings::RLE_DICTIONARY,
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
        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        (new PlainValuesPacker($pageWriter, $this->byteOrder))->packValues($this->column, $dictionary->dictionary);

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            strlen($compressedBuffer),
            strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(Encodings::PLAIN, count($dictionary->dictionary)),
        );

        return new PageContainer($compressedBuffer, $pageHeader);
    }

    /**
     * The page's values in the chunk dictionary, extending it; null when that would grow it past
     * Option::DICTIONARY_PAGE_SIZE, which turns this page and the rest of the chunk PLAIN.
     *
     * @return null|array<int, int>
     */
    private function dictionaryIndices(): ?array
    {
        $extended = (new DictionaryBuilder())->build(
            $this->column,
            new WriteFlatColumnValues(
                $this->column,
                $this->repetitionLevels,
                $this->definitionLevels,
                $this->pageValues,
            ),
            $this->dictionary,
        );
        $added = '';
        (new PlainValuesPacker(new BinaryBufferWriter($added), $this->byteOrder))->packValues(
            $this->column,
            array_slice($extended->dictionary, count($this->dictionary->dictionary)),
        );

        if (($this->dictionaryBytes + strlen($added)) > $this->options->getInt(Option::DICTIONARY_PAGE_SIZE)) {
            $this->plain = true;

            return null;
        }

        $this->dictionary = $extended;
        $this->dictionaryBytes += strlen($added);

        return $extended->indices;
    }

    /**
     * @param null|array<int, int> $indices
     */
    private function values(RLEBitPackedPacker $packer, ?array $indices): string
    {
        if ($indices === null) {
            $values = '';
            (new PlainValuesPacker(new BinaryBufferWriter($values), $this->byteOrder))->packValues(
                $this->column,
                $this->pageValues,
            );

            return $values;
        }

        return $indices === [] ? '' : $packer->packWithBitWidth(BitWidth::fromArray($indices), $indices);
    }

    private function currentPageUncompressedSize(): int
    {
        return (
            (count($this->pageValues) * 4) + (count($this->repetitionLevels) * 4) + (count($this->definitionLevels) * 4)
        );
    }
}
