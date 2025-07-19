<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\{
    Data\BitWidth,
    Data\PlainValuesPacker,
    Dremel\WriteColumnData,
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
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\{Compressions,
    Encodings
};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn};
use Flow\Parquet\Writer\PageBuilder\{Dictionary, DictionaryBuilder};
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class RLEDictionaryChunkBuilder implements ColumnChunkBuilder
{
    private StatisticsCounter $chunkStatistics;

    /**
     * @var array<int>
     */
    private array $definitionLevels = [];

    private ?Dictionary $dictionary = null;

    private int $nullCount = 0;

    private readonly PageContainers $pages;

    private StatisticsCounter $pageStatistics;

    /**
     * @var array<null|bool|float|int|string>
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
            }
        }

        array_push($this->pageValues, ...$flatValues->values());

        foreach ($flatValues->values() as $value) {
            $this->pageStatistics->add($value);
        }

        $this->rowsCount++;
    }

    public function closePage() : void
    {
        $codec = new Codec($this->options);

        if (\count($this->pageValues) > 0) {
            $flatColumnValues = new WriteFlatColumnValues(
                $this->column,
                $this->repetitionLevels,
                $this->definitionLevels,
                $this->pageValues
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
            default => throw new RuntimeException('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: ' . $writerVersion),
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

    public function column() : Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset) : array
    {
        if (\count($this->pageValues) > 0 || \count($this->definitionLevels) > 0) {
            $this->closePage();
        }

        return [new ColumnChunkContainer(
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
                dictionaryPageOffset: ($this->pages->dictionaryPageContainer()) ? $fileOffset : null,
                dataPageOffset: ($this->pages->dictionaryPageContainer()) ? $fileOffset + $this->pages->dictionaryPageContainer()->totalCompressedSize() : $fileOffset,
                indexPageOffset: null,
                statistics: $this->chunkStatistics->toStatistics(),
                options: $this->options
            )
        )];
    }

    public function isFull() : bool
    {
        return \count($this->pageValues) * 4 >= $this->options->get(Option::PAGE_SIZE_BYTES);
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

        if ($this->dictionary && \count($this->dictionary->indices) > 0) {
            $bitWidth = BitWidth::fromArray($this->dictionary->indices);
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth($bitWidth, $this->dictionary->indices));
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
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $this->dictionary->indices ?? [],
            $this->dictionary->dictionary ?? [],
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

        $indicesBuffer = '';

        if ($this->dictionary && \count($this->dictionary->indices) > 0) {
            $bitWidth = BitWidth::fromArray($this->dictionary->indices);
            $indicesBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth($bitWidth, $this->dictionary->indices);
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
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $repetitionsBuffer . $definitionsBuffer . $compressedBuffer,
            $this->dictionary->indices ?? [],
            $this->dictionary->dictionary ?? [],
            $pageHeader
        );
    }

    private function buildDictionaryPage(Codec $codec, Compressions $compression) : PageContainer
    {
        if (!$this->dictionary) {
            throw new RuntimeException('Cannot build dictionary page without dictionary');
        }

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        (new PlainValuesPacker($pageWriter))->packValues($this->column, $this->dictionary->dictionary);

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(
                Encodings::PLAIN,
                \count($this->dictionary->dictionary)
            ),
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $this->dictionary->indices,
            $this->dictionary->dictionary,
            $pageHeader
        );
    }

    private function currentPageUncompressedSize() : int
    {
        return (count($this->pageValues) * 4) + (count($this->repetitionLevels) * 4) + (count($this->definitionLevels) * 4);
    }
}
