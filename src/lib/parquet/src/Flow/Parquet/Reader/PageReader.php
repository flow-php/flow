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
                $dataPageHeader = $pageHeader->dataPageHeader();

                if ($dataPageHeader === null) {
                    throw new RuntimeException('DATA_PAGE header is missing data page header');
                }

                $data = (new Codec($this->options))->decompress(
                    $this->readBytes($stream, $pageHeader->compressedPageSize()),
                    $codec,
                    $pageHeader->uncompressedPageSize(),
                );

                return (new ColumnDataDecoder($this->byteOrder))->decodeData(
                    $data,
                    $column,
                    $dataPageHeader,
                    $dictionary,
                );
            case Type::DATA_PAGE_V2:
                $dataPageHeaderV2 = $pageHeader->dataPageHeaderV2();

                if ($dataPageHeaderV2 === null) {
                    throw new RuntimeException('DATA_PAGE_V2 header is missing data page header v2');
                }

                $levelsLength = $dataPageHeaderV2->repetitionsByteLength() + $dataPageHeaderV2->definitionsByteLength();

                $levels = $levelsLength > 0 ? $this->readBytes($stream, $levelsLength) : '';

                $data = (new Codec($this->options))->decompress(
                    $this->readBytes($stream, $pageHeader->compressedPageSize() - $levelsLength),
                    $codec,
                    $pageHeader->uncompressedPageSize() - $levelsLength,
                );

                return (new ColumnDataDecoder($this->byteOrder))->decodeDataV2(
                    $levels . $data,
                    $column,
                    $dataPageHeaderV2,
                    $dictionary,
                );

            default:
                throw new RuntimeException("Unknown page header type '{$pageHeader->type()->name}'");
        }
    }

    /**
     * @param resource $stream
     */
    public function readDictionary(FlatColumn $column, PageHeader $pageHeader, Compressions $codec, $stream): Dictionary
    {
        $dictionaryPageHeader = $pageHeader->dictionaryPageHeader();

        if ($dictionaryPageHeader === null) {
            throw new RuntimeException("Can't read dictionary from non dictionary page header");
        }

        $compressedSize = $pageHeader->compressedPageSize();
        $compressed = $compressedSize === 0 ? '' : $this->readBytes($stream, $compressedSize);

        return (new ColumnDataDecoder($this->byteOrder))->decodeDictionary(
            (new Codec($this->options))->decompress($compressed, $codec, $pageHeader->uncompressedPageSize()),
            $column,
            $dictionaryPageHeader,
        );
    }

    /**
     * @param resource $stream
     */
    private function readBytes($stream, int $length): string
    {
        if ($length <= 0) {
            return '';
        }

        $bytes = \fread($stream, $length);

        if ($bytes === false) {
            throw new RuntimeException("Failed to read {$length} bytes from page stream");
        }

        return $bytes;
    }
}
