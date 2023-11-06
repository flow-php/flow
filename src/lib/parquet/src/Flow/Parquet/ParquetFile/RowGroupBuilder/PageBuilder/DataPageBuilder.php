<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Codec;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DataPageBuilder
{
    public function __construct(
        private readonly DataConverter $dataConverter,
        private readonly Compressions $compression,
        private readonly Options $options,
    ) {
    }

    public function build(FlatColumn $column, array $rows, ?array $dictionary = null, ?array $indices = null) : PageContainer
    {
        switch ($this->options->get(Option::WRITER_VERSION)) {
            case 1:
                return $this->buildDataPage($rows, $column, $dictionary, $indices);
            case 2:
                return $this->buildDataPageV2($rows, $column, $dictionary, $indices);

            default:
                throw new \RuntimeException('Flow Parquet Writer does not support given version of Parquet format, supported versions are [1,2], given: ' . $this->options->get(Option::WRITER_VERSION));
        }
    }

    private function buildDataPage(array $rows, FlatColumn $column, ?array $dictionary, ?array $indices) : PageContainer
    {
        $shredded = (new Dremel())->shred($rows, $column->maxDefinitionsLevel());

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength($shredded->repetitions));
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithLength($shredded->definitions));
        }

        if ($dictionary && $indices) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth($indices));
        } else {
            (new PlainValuesPacker($pageWriter, $this->dataConverter))->packValues($column, $shredded->values);
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
                valuesCount: \count($shredded->definitions)
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $shredded->values,
            null,
            $pageHeader
        );
    }

    private function buildDataPageV2(array $rows, FlatColumn $column, ?array $dictionary, ?array $indices) : PageContainer
    {
        $statistics = new DataPageV2Statistics();

        foreach ($rows as $row) {
            $statistics->add($row);
        }

        $statistics = (new StatisticsBuilder($this->dataConverter))->build($column, $statistics);

        $shredded = (new Dremel())->shred($rows, $column->maxDefinitionsLevel());

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $repetitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack($shredded->repetitions);
            $repetitionsLength = \strlen($repetitionsBuffer);
        } else {
            $repetitionsBuffer = '';
            $repetitionsLength = 0;
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $definitionsBuffer = (new RLEBitPackedPacker($rleBitPackedHybrid))->pack($shredded->definitions);
            $definitionsLength = \strlen($definitionsBuffer);
        } else {
            $definitionsBuffer = '';
            $definitionsLength = 0;
        }

        if ($dictionary && $indices) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth($indices));
        } else {
            (new PlainValuesPacker($pageWriter, $this->dataConverter))->packValues($column, $shredded->values);
        }

        $compressedBuffer = (new Codec($this->options))->compress($pageBuffer, $this->compression);

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            \strlen($compressedBuffer) + $repetitionsLength + $definitionsLength,
            \strlen($pageBuffer) + $repetitionsLength + $definitionsLength,
            dataPageHeader: null,
            dataPageHeaderV2: new DataPageHeaderV2(
                valuesCount: \count($shredded->definitions),
                nullsCount: \count(\array_filter($shredded->definitions, fn (int $definition) : bool => $definition === 0)),
                rowsCount: \count($rows),
                encoding: (\is_array($dictionary) && \is_array($indices)) ? Encodings::RLE_DICTIONARY : Encodings::PLAIN,
                definitionsByteLength: $definitionsLength,
                repetitionsByteLength: $repetitionsLength,
                isCompressed: !($this->compression === Compressions::UNCOMPRESSED),
                statistics: $statistics,
            ),
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $repetitionsBuffer . $definitionsBuffer . $compressedBuffer,
            $shredded->values,
            null,
            $pageHeader
        );
    }
}
