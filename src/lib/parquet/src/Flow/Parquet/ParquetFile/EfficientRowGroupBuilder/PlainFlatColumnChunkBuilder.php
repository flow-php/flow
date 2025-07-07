<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\EfficientRowGroupBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Codec, Compressions, Encodings};
use Flow\Parquet\ParquetFile\Data\{BitWidth, PlainValuesPacker, RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{ColumnChunkContainer, PageContainer, PageContainers, WriteColumnData};
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\{RLEBitPackedPacker};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn};
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class PlainFlatColumnChunkBuilder implements ColumnChunkBuilder
{
    private StatisticsCounter $chunkStatistics;

    /**
     * @var array<int>
     */
    private array $definitionLevels = [];

    private int $nullCount = 0;

    private readonly PageContainers $pages;

    private StatisticsCounter $pageStatistics;

    private string $pageValueBuffer = '';

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

        $buffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($buffer)))->packValues($this->column, $flatValues->values());

        $this->pageValueBuffer .= $buffer;

        foreach ($flatValues->values() as $value) {
            $this->pageStatistics->add($value);
        }

        $this->rowsCount++;

        if (\strlen($this->pageValueBuffer) >= $this->options->get(Option::PAGE_SIZE_BYTES)) {
            $this->closePage(new Codec($this->options), $this->compression);
        }
    }

    public function column() : Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset) : array
    {
        if ($this->pageValueBuffer !== '') {
            $this->closePage(new Codec($this->options), $this->compression);
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

    public function uncompressedSize() : int
    {
        return $this->pages->uncompressedSize();
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

        $pageWriter->append($this->pageValueBuffer);

        $compressedBuffer = $codec->compress($pageBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                encoding: Encodings::PLAIN,
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
            [],
            null,
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

        $compressedBuffer = $codec->compress($this->pageValueBuffer, $compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            \strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            \strlen($this->pageValueBuffer) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: \count($this->definitionLevels),
                nullsCount: $this->nullCount,
                rowsCount: $this->rowsCount,
                encoding: Encodings::PLAIN,
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
            [],
            null,
            $pageHeader
        );
    }

    private function closePage(Codec $codec, Compressions $compression) : void
    {
        $pageContainer = match ($writerVersion = $this->options->getInt(Option::WRITER_VERSION)) {
            1 => $this->buildDataPage($codec, $compression),
            2 => $this->buildDataPageV2($codec, $compression),
            default => throw new \RuntimeException('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: ' . $writerVersion),
        };

        $this->pages->add($pageContainer);
        $this->chunkStatistics = $this->chunkStatistics->merge($this->pageStatistics);

        $this->repetitionLevels = [];
        $this->definitionLevels = [];
        $this->pageValueBuffer = '';
        $this->rowsCount = 0;
        $this->nullCount = 0;
        $this->pageStatistics = new StatisticsCounter($this->column);
    }
}
