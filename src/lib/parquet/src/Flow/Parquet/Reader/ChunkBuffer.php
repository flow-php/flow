<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

use function fopen;
use function fseek;
use function ftell;
use function fwrite;
use function max;
use function rewind;

use const SEEK_END;

final readonly class ChunkBuffer
{
    public function __construct(
        private Options $options,
    ) {}

    /**
     * @return resource the chunk's bytes in php://temp, rewound; PARQUET-816 dictionary bytes included
     */
    public function of(ColumnChunk $columnChunk, SourceStream $stream)
    {
        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        $compressedSize = $columnChunk->totalCompressedSize();

        if ($compressedSize <= 0) {
            throw new RuntimeException('Cannot read column chunk with non-positive compressed size');
        }

        fwrite($pageStream, $stream->read($compressedSize, $columnChunk->pageOffset()));
        rewind($pageStream);

        // PARQUET-816: old parquet-mr writers set data_page_offset to the dictionary page and left
        // dictionary_page_offset null, excluding the dictionary page header size from total_compressed_size
        if ($columnChunk->dictionaryPageOffset() === null) {
            $header = PageHeader::read($pageStream, $this->options);

            if ($header !== null && $header->type()->isDictionaryPage()) {
                $headerSize = max(1, (int) ftell($pageStream));
                fseek($pageStream, 0, SEEK_END);
                fwrite($pageStream, $stream->read($headerSize, $columnChunk->pageOffset() + $compressedSize));
            }

            rewind($pageStream);
        }

        return $pageStream;
    }
}
