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
use function fwrite;
use function rewind;

final readonly class ColumnChunkViewer
{
    public function __construct(
        private Options $options,
    ) {}

    /**
     * @return \Generator<PageHeader>
     */
    public function view(ColumnChunk $columnChunk, SourceStream $stream): Generator
    {
        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        $compressedSize = $columnChunk->totalCompressedSize();

        if ($compressedSize <= 0) {
            throw new RuntimeException('Cannot view column chunk with non-positive compressed size');
        }

        fwrite($pageStream, $stream->read($compressedSize, $columnChunk->pageOffset()));
        rewind($pageStream);

        if ($columnChunk->dictionaryPageOffset()) {
            $dictionaryHeader = PageHeader::read($pageStream, $this->options);

            if ($dictionaryHeader === null) {
                throw new RuntimeException(
                    'Dictionary page header not found in column chunk under offset: ' . $columnChunk->pageOffset(),
                );
            }

            $currentPosition = ftell($pageStream);

            if ($currentPosition === false) {
                throw new RuntimeException('Cannot determine current page stream position');
            }
            fseek($pageStream, $currentPosition + $dictionaryHeader->compressedPageSize());

            yield $dictionaryHeader;
        }

        while (true) {
            $dataHeader = PageHeader::read($pageStream, $this->options);

            /** There are no more pages in given column chunk */
            if ($dataHeader === null || $dataHeader->type()->isDataPage() === false) {
                break;
            }

            $currentPosition = ftell($pageStream);

            if ($currentPosition === false) {
                throw new RuntimeException('Cannot determine current page stream position');
            }
            fseek($pageStream, $currentPosition + $dataHeader->compressedPageSize());

            yield $dataHeader;
        }

        fclose($pageStream);
    }
}
