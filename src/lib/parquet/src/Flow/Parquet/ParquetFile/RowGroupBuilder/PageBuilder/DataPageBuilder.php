<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\{BitWidth, PlainValuesPacker, RLEBitPackedHybrid};
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\{Codec,
    Compressions,
    Encodings,
    RowGroupBuilder\ColumnData\FlatColumnValues};
use Flow\Parquet\{Option, Options};
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final readonly class DataPageBuilder
{
    public function __construct(
        private Compressions $compression,
        private Options $options,
    ) {
    }

    public function build(FlatColumn $column, FlatColumnValues $rows, ?array $dictionary = null, ?array $indices = null) : PageContainer
    {
        return match ($this->options->get(Option::WRITER_VERSION)) {
            1 => $this->buildDataPage($rows, $column, $dictionary, $indices),
            2 => $this->buildDataPageV2($rows, $column, $dictionary, $indices),
            default => throw new \RuntimeException('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: ' . $this->options->get(Option::WRITER_VERSION)),
        };
    }

    private function buildDataPage(FlatColumnValues $data, FlatColumn $column, ?array $dictionary, ?array $indices) : PageContainer
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength(BitWidth::calculate($column->maxRepetitionsLevel()), $data->repetitionLevels()));
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength(BitWidth::calculate($column->maxDefinitionsLevel()), $data->definitionLevels()));
        }

        if ($dictionary && $indices) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth(BitWidth::fromArray($indices), $indices));
        } else {
            (new PlainValuesPacker($pageWriter))->packValues($column, $data->values());
        }

        $compressedBuffer = (new Codec($this->options))->compress($pageBuffer, $this->compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                encoding: (\is_array($dictionary) && \is_array($indices)) ? Encodings::RLE_DICTIONARY : Encodings::PLAIN,
                repetitionLevelEncoding: Encodings::RLE,
                definitionLevelEncoding: Encodings::RLE,
                valuesCount: $data->definitionLevelsCount(),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $data->values(),
            null,
            $pageHeader
        );
    }

    private function buildDataPageV2(FlatColumnValues $data, FlatColumn $column, ?array $dictionary, ?array $indices) : PageContainer
    {
        $pageStatistics = new DataPageV2Statistics();

        foreach ($data->values() as $value) {
            $pageStatistics->add($value);
        }

        $statistics = (new StatisticsBuilder())->build($column, $pageStatistics);

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack(BitWidth::calculate($column->maxRepetitionsLevel()), $data->repetitionLevels());
            $repetitionsLength = \strlen($repetitionsBuffer);
        } else {
            $repetitionsBuffer = '';
            $repetitionsLength = 0;
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack(BitWidth::calculate($column->maxDefinitionsLevel()), $data->definitionLevels());
            $definitionsLength = \strlen($definitionsBuffer);
        } else {
            $definitionsBuffer = '';
            $definitionsLength = 0;
        }

        if ($dictionary && $indices) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth(BitWidth::fromArray($indices), $indices));
        } else {
            (new PlainValuesPacker($pageWriter))->packValues($column, $data->values());
        }

        $compressedBuffer = (new Codec($this->options))->compress($pageBuffer, $this->compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            \strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            \strlen($pageBuffer) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: $data->definitionLevelsCount(),
                nullsCount: $data->nullCount(),
                rowsCount: $data->rowsCount(),
                encoding: (\is_array($dictionary) && \is_array($indices)) ? Encodings::RLE_DICTIONARY : Encodings::PLAIN,
                definitionsByteLength: $definitionsLength,
                repetitionsByteLength: $repetitionsLength,
                isCompressed: !($this->compression === Compressions::UNCOMPRESSED),
                statistics: $statistics,
                options: $this->options
            ),
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $repetitionsBuffer . $definitionsBuffer . $compressedBuffer,
            $data->values(),
            null,
            $pageHeader
        );
    }
}
