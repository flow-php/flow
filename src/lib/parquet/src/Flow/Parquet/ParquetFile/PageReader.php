<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ByteOrder;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class PageReader
{
    public function __construct(
        private readonly ByteOrder $byteOrder,
        private readonly Options $options
    ) {
    }

    /**
     * @param resource $stream
     *
     * @psalm-suppress PossiblyNullReference
     * @psalm-suppress PossiblyNullArgument
     */
    public function readData(FlatColumn $column, PageHeader $pageHeader, Compressions $codec, ?Dictionary $dictionary, $stream) : ColumnData
    {
        switch ($pageHeader->type()) {
            case Type::DATA_PAGE:
                $data = (new Codec($this->options))
                    ->decompress(
                        /** @phpstan-ignore-next-line */
                        \fread($stream, $pageHeader->compressedPageSize()),
                        $codec
                    );

                /** @phpstan-ignore-next-line  */
                return (new DataCoder($this->byteOrder))->decodeData($data, $column, $pageHeader->dataPageHeader(), $dictionary);
            case Type::DATA_PAGE_V2:

                /* @phpstan-ignore-next-line */
                $levelsLength = $pageHeader->dataPageHeaderV2()->repetitionsByteLength() + $pageHeader->dataPageHeaderV2()->definitionsByteLength();

                if ($levelsLength) {
                    /* @phpstan-ignore-next-line */
                    $levels = \fread($stream, $levelsLength);
                } else {
                    $levels = '';
                }

                $data = (new Codec($this->options))
                    ->decompress(
                        /** @phpstan-ignore-next-line */
                        \fread($stream, $pageHeader->compressedPageSize() - $levelsLength),
                        $codec
                    );

                return (new DataCoder($this->byteOrder))
                    /** @phpstan-ignore-next-line */
                    ->decodeDataV2($levels . $data, $column, $pageHeader->dataPageHeaderV2(), $dictionary);

            default:
                throw new RuntimeException("Unknown page header type '{$pageHeader->type()->name}'");
        }
    }

    /**
     * @param resource $stream
     */
    public function readDictionary(FlatColumn $column, PageHeader $pageHeader, Compressions $codec, $stream) : Dictionary
    {
        if (!$pageHeader->dictionaryPageHeader()) {
            throw new RuntimeException("Can't read dictionary from non dictionary page header");
        }

        return (new DataCoder($this->byteOrder))
            ->decodeDictionary(
                (new Codec($this->options))
                    ->decompress(
                        /** @phpstan-ignore-next-line */
                        \fread($stream, $pageHeader->compressedPageSize()),
                        $codec
                    ),
                $column,
                $pageHeader->dictionaryPageHeader()
            );
    }
}
