<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Generator;

use function fclose;
use function fseek;
use function ftell;

final readonly class ColumnChunkViewer
{
    private ChunkBuffer $buffer;

    public function __construct(
        private Options $options,
    ) {
        $this->buffer = new ChunkBuffer($options);
    }

    /**
     * The chunk's dictionary page header, if any, then its data page headers.
     *
     * @return \Generator<PageHeader>
     */
    public function view(ColumnChunk $columnChunk, SourceStream $stream): Generator
    {
        $pageStream = $this->buffer->of($columnChunk, $stream);

        while (true) {
            $header = PageHeader::read($pageStream, $this->options);

            /** There are no more pages in given column chunk */
            if ($header === null || !$header->type()->isDataPage() && !$header->type()->isDictionaryPage()) {
                break;
            }

            $currentPosition = ftell($pageStream);

            if ($currentPosition === false) {
                throw new RuntimeException('Cannot determine current page stream position');
            }

            fseek($pageStream, $currentPosition + $header->compressedPageSize());

            yield $header;
        }

        fclose($pageStream);
    }
}
