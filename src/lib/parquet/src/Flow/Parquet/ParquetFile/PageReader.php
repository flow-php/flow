<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ByteOrder;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class PageReader
{
    public function __construct(
        private readonly FlatColumn $column,
        private readonly ByteOrder $byteOrder,
        private readonly Options $options
    ) {
    }

    /**
     * @param resource $stream
     *
     * @psalm-suppress PossiblyNullReference
     */
    public function readData(PageHeader $pageHeader, Compressions $codec, ?Dictionary $dictionary, $stream) : ColumnData
    {
        return (new DataCoder($this->byteOrder))
            ->decodeData(
                (new Codec($this->options))
                    ->decompress(
                        /** @phpstan-ignore-next-line */
                        \fread($stream, $pageHeader->compressedPageSize()),
                        $codec
                    ),
                /** @phpstan-ignore-next-line  */
                $pageHeader->dataPageHeader()->encoding(),
                $this->column->type(),
                $this->column->logicalType(),
                /** @phpstan-ignore-next-line  */
                $pageHeader->dataPageHeader()->valuesCount(),
                $this->column->maxRepetitionsLevel(),
                $this->column->maxDefinitionsLevel(),
                $this->column->typeLength(),
                $dictionary
            );
    }

    /**
     * @param resource $stream
     */
    public function readDictionary(PageHeader $pageHeader, Compressions $codec, $stream) : Dictionary
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
                $this->column->type(),
                $this->column->logicalType(),
                $pageHeader->dictionaryPageHeader()->encoding(),
                $pageHeader->dictionaryPageHeader()->valuesCount(),
                $this->column->typeLength(),
            );
    }
}
