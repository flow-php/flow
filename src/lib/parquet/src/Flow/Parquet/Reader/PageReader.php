<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\Codec;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class PageReader
{
    public function __construct(
        private ByteOrder $byteOrder,
        private Options $options,
    ) {}

    /**
     * @param resource $stream
     */
    public function readData(
        FlatColumn $column,
        PageHeader $pageHeader,
        Compressions $codec,
        ?Dictionary $dictionary,
        $stream,
    ): ReadFlatColumnValues {
        switch ($pageHeader->type()) {
            case Type::DATA_PAGE:
                $data = (new Codec($this->options))->decompress(
                    /** @phpstan-ignore-next-line */
                    \fread($stream, $pageHeader->compressedPageSize()),
                    $codec,
                    $pageHeader->uncompressedPageSize(),
                );

                return (new ColumnDataDecoder($this->byteOrder))->decodeData(
                    $data,
                    $column,
                    /** @phpstan-ignore-next-line */
                    $pageHeader->dataPageHeader(),
                    $dictionary,
                );
            case Type::DATA_PAGE_V2:
                $levelsLength =
                    /** @phpstan-ignore-next-line */
                    $pageHeader->dataPageHeaderV2()->repetitionsByteLength() /** @phpstan-ignore-next-line */
                    + $pageHeader->dataPageHeaderV2()->definitionsByteLength();

                if ($levelsLength) {
                    /* @phpstan-ignore-next-line */
                    $levels = \fread($stream, $levelsLength);
                } else {
                    $levels = '';
                }

                $data = (new Codec($this->options))->decompress(
                    /** @phpstan-ignore-next-line */
                    \fread($stream, $pageHeader->compressedPageSize() - $levelsLength),
                    $codec,
                    $pageHeader->uncompressedPageSize() - $levelsLength,
                );

                return (new ColumnDataDecoder($this->byteOrder))
                    /** @phpstan-ignore-next-line */
                    ->decodeDataV2($levels . $data, $column, $pageHeader->dataPageHeaderV2(), $dictionary);

            default:
                throw new RuntimeException("Unknown page header type '{$pageHeader->type()->name}'");
        }
    }

    /**
     * @param resource $stream
     */
    public function readDictionary(FlatColumn $column, PageHeader $pageHeader, Compressions $codec, $stream): Dictionary
    {
        if (!$pageHeader->dictionaryPageHeader()) {
            throw new RuntimeException("Can't read dictionary from non dictionary page header");
        }

        return (new ColumnDataDecoder($this->byteOrder))->decodeDictionary(
            (new Codec($this->options))->decompress(
                /** @phpstan-ignore-next-line */
                $pageHeader->compressedPageSize() === 0 ? '' : \fread($stream, $pageHeader->compressedPageSize()),
                $codec,
                $pageHeader->uncompressedPageSize(),
            ),
            $column,
            $pageHeader->dictionaryPageHeader(),
        );
    }
}
