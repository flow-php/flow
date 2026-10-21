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
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
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
use Flow\Parquet\Writer\ValueStorage\ValueStorage;

use function array_filter;
use function array_push;
use function array_slice;
use function count;
use function implode;
use function intdiv;
use function is_string;
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

    /**
     * Built once the dictionary overflows, when it can no longer change, so its values are freed before the flush.
     */
    private ?PageContainer $dictionaryPage = null;

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
     * Set once a page would grow the dictionary to Option::DICTIONARY_PAGE_SIZE: that page and the rest of the chunk
     * use the fallback storage's encoding, as arrow-rs falls back.
     */
    private bool $overflowed = false;

    /**
     * Set once a page of the chunk is written with dictionary indices, which needs a dictionary page even when empty.
     */
    private bool $indexed = false;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly Options $options,
        private readonly Compressions $compression,
        private readonly StatisticsOptions $statistics,
        private readonly ValueStorage $fallback,
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

        if ($this->overflowed) {
            $this->fallback->addValues($this->column, $columnValues->values());
        } else {
            array_push($this->pageValues, ...$columnValues->values());
        }

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
        $indices = $this->overflowed ? null : $this->dictionaryIndices();

        if ($indices !== null) {
            $this->indexed = true;
        }

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
        $this->fallback->reset();
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

        if ($this->dictionaryPage !== null) {
            $this->pages->add($this->dictionaryPage);
        } elseif ($this->indexed) {
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
        $this->fallback->reset();
        $this->dictionary = new Dictionary([], []);
        $this->dictionaryBytes = 0;
        $this->dictionaryPage = null;
        $this->overflowed = false;
        $this->indexed = false;
        $this->rowsCount = 0;
        $this->nullCount = 0;

        return $containers;
    }

    public function isEmpty(): bool
    {
        return (
            count($this->pageValues) === 0
            && $this->fallback->isEmpty()
            && count($this->definitionLevels) === 0
            && count($this->repetitionLevels) === 0
        );
    }

    public function isFull(): bool
    {
        return $this->pageValuesSize() >= $this->options->getInt(Option::PAGE_SIZE_BYTES);
    }

    public function uncompressedSize(): int
    {
        return $this->pages->uncompressedSize() + $this->currentPageUncompressedSize();
    }

    /**
     * @param null|array<int, int> $indices the page's dictionary indices, null for a fallback page
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
                encoding: $indices === null ? $this->fallback->encoding() : Encodings::RLE_DICTIONARY,
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
     * @param null|array<int, int> $indices the page's dictionary indices, null for a fallback page
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
                encoding: $indices === null ? $this->fallback->encoding() : Encodings::RLE_DICTIONARY,
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
     * The page's values in the chunk dictionary, extending it; null when that would grow it to
     * Option::DICTIONARY_PAGE_SIZE, which turns this page and the rest of the chunk to the fallback encoding.
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
        $addedBytes = $this->plainSize(array_slice($extended->dictionary, count($this->dictionary->dictionary)));

        if (($this->dictionaryBytes + $addedBytes) >= $this->options->getInt(Option::DICTIONARY_PAGE_SIZE)) {
            $this->overflowed = true;

            if ($this->indexed) {
                $this->dictionaryPage = $this->buildDictionaryPage(new Codec($this->options), $this->compression);
            }

            $this->dictionary = new Dictionary([], []);

            return null;
        }

        $this->dictionary = $extended;
        $this->dictionaryBytes += $addedBytes;

        return $extended->indices;
    }

    /**
     * @param null|array<int, int> $indices
     */
    private function values(RLEBitPackedPacker $packer, ?array $indices): string
    {
        if ($indices === null) {
            $this->fallback->addValues($this->column, $this->pageValues);

            return $this->fallback->getBuffer();
        }

        return $packer->packWithBitWidth(BitWidth::fromArray($indices), $indices);
    }

    private function currentPageUncompressedSize(): int
    {
        return $this->pageValuesSize() + (count($this->repetitionLevels) * 4) + (count($this->definitionLevels) * 4);
    }

    private function pageValuesSize(): int
    {
        return $this->overflowed ? $this->fallback->plainSize() : count($this->pageValues) * 4;
    }

    /**
     * @param array<mixed> $values
     */
    private function plainSize(array $values): int
    {
        return match ($this->column->type()) {
            PhysicalType::BYTE_ARRAY => (count($values) * 4)
                + strlen(implode('', array_filter($values, is_string(...)))),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => strlen(implode('', array_filter($values, is_string(...)))),
            PhysicalType::BOOLEAN => intdiv(count($values) + 7, 8),
            PhysicalType::INT32, PhysicalType::FLOAT => count($values) * 4,
            PhysicalType::INT64, PhysicalType::DOUBLE => count($values) * 8,
            PhysicalType::INT96 => count($values) * 12,
        };
    }
}
