<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Dremel\Dremel;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
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

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            \strlen($pageBuffer),
            \strlen($pageBuffer),
            dataPageHeader: new DataPageHeader(
                $dictionary && $indices ? Encodings::RLE_DICTIONARY : Encodings::PLAIN,
                $dictionary && $indices ? \count($indices) : \count($shredded->values),
            ),
            dataPageHeaderV2: null,
            dictionaryPageHeader: null,
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $pageBuffer,
            $shredded->values,
            null,
            $pageHeader
        );
    }
}
