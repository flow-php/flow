<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\PhpFileStream;

final readonly class ColumnChunkViewer
{
    public function __construct(
        private Options $options,
    ) {}

    /**
     * @return \Generator<PageHeader>
     */
    public function view(ColumnChunk $columnChunk, SourceStream $stream): \Generator
    {
        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        $compressedSize = $columnChunk->totalCompressedSize();

        if ($compressedSize <= 0) {
            throw new RuntimeException('Cannot view column chunk with non-positive compressed size');
        }

        \fwrite($pageStream, $stream->read($compressedSize, $columnChunk->pageOffset()));
        \rewind($pageStream);

        if ($columnChunk->dictionaryPageOffset()) {
            $dictionaryHeader = $this->readHeader($pageStream);

            if ($dictionaryHeader === null) {
                throw new RuntimeException(
                    'Dictionary page header not found in column chunk under offset: ' . $columnChunk->pageOffset(),
                );
            }

            yield $dictionaryHeader;
        }

        while (true) {
            $dataHeader = $this->readHeader($pageStream);

            /** There are no more pages in given column chunk */
            if ($dataHeader === null || $dataHeader->type()->isDataPage() === false) {
                break;
            }

            $currentPosition = \ftell($pageStream);

            if ($currentPosition === false) {
                throw new RuntimeException('Cannot determine current page stream position');
            }
            \fseek($pageStream, $currentPosition + $dataHeader->compressedPageSize());

            yield $dataHeader;
        }

        \fclose($pageStream);
    }

    /**
     * @param resource $stream
     */
    private function readHeader($stream): ?PageHeader
    {
        $currentOffset = \ftell($stream);

        try {
            $thriftHeader = new \Flow\Parquet\ThriftModel\PageHeader();
            @$thriftHeader->read(new CompactProtocol(new PhpFileStream($stream)));

            return PageHeader::fromThrift($thriftHeader, $this->options);
        } catch (\Throwable) {
            if ($currentOffset !== false) {
                \fseek($stream, $currentOffset);
            }

            return null;
        }
    }
}
