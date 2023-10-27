<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageContainer;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TMemoryBuffer;

final class DictionaryPageBuilder
{
    public function __construct(private readonly DataConverter $dataConverter)
    {
    }

    public function build(FlatColumn $column, array $rows) : PageContainer
    {
        $dictionary = (new DictionaryBuilder())->build($column, $rows);

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        $pageWriter->append((new PlainValuesPacker($this->dataConverter))->packValues($column, $dictionary->dictionary));

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            \strlen($pageBuffer),
            \strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(
                Encodings::PLAIN,
                \count($dictionary->dictionary)
            ),
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $pageBuffer,
            $dictionary->indices,
            $dictionary->dictionary,
            $pageHeader
        );
    }
}
