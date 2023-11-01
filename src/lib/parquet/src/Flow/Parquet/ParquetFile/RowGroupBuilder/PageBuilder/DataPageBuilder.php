<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Codec;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
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
        $shredded = (new Dremel())->shred($rows, $column->maxDefinitionsLevel());

        $rleBitPackedHybrid = new RLEBitPackedHybrid();

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);

        if ($column->maxRepetitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->pack($shredded->repetitions));
        }

        if ($column->maxDefinitionsLevel() > 0) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->pack($shredded->definitions));
        }

        if ($dictionary && $indices) {
            $pageWriter->append((new RLEBitPackedPacker($rleBitPackedHybrid))->packWithBitWidth($indices));
        } else {
            $pageWriter->append((new PlainValuesPacker($this->dataConverter))->packValues($column, $shredded->values));
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
}
