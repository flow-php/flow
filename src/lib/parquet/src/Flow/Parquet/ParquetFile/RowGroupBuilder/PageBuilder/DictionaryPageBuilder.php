<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Codec;
use Flow\Parquet\ParquetFile\Compressions;
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
    public function __construct(
        private readonly DataConverter $dataConverter,
        private readonly Compressions $compression,
        private readonly Options $options,
    ) {
    }

    public function build(FlatColumn $column, array $rows) : PageContainer
    {
        $dictionary = (new DictionaryBuilder())->build($column, $rows);

        $pageBuffer = '';
        $pageWriter = new BinaryBufferWriter($pageBuffer);
        $pageWriter->append((new PlainValuesPacker($this->dataConverter))->packValues($column, $dictionary->dictionary));

        $compressedBuffer = (new Codec($this->options))->compress($pageBuffer, $this->compression);

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            \strlen($compressedBuffer),
            \strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: new DictionaryPageHeader(
                Encodings::PLAIN_DICTIONARY,
                \count($dictionary->dictionary)
            ),
        );
        $pageHeader->toThrift()->write(new TCompactProtocol($pageHeaderBuffer = new TMemoryBuffer()));

        return new PageContainer(
            $pageHeaderBuffer->getBuffer(),
            $compressedBuffer,
            $dictionary->indices,
            $dictionary->dictionary,
            $pageHeader
        );
    }
}
