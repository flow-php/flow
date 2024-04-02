<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\ColumnChunkViewer;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\ColumnChunkViewer;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TBufferedTransport;

final class WholeChunkViewer implements ColumnChunkViewer
{
    /**
     * @param resource $stream
     */
    public function view(ColumnChunk $columnChunk, FlatColumn $column, $stream) : \Generator
    {
        $offset = $columnChunk->pageOffset();

        \fseek($stream, $offset);

        if ($columnChunk->dictionaryPageOffset()) {
            $dictionaryHeader = $this->readHeader($stream, $offset);

            if ($dictionaryHeader === null) {
                throw new RuntimeException('Dictionary page header not found in column chunk under offset: ' . $offset);
            }

            yield $dictionaryHeader;

            $offset += $dictionaryHeader->compressedPageSize();
        }

        while (true) {
            $dataHeader = $this->readHeader($stream, $offset);

            /** There are no more pages in given column chunk */
            if ($dataHeader === null || $dataHeader->type()->isDataPage() === false) {
                break;
            }

            yield $dataHeader;

            $offset += $dataHeader->compressedPageSize();
        }
    }

    /**
     * @param resource $stream
     */
    private function readHeader($stream, int $pageOffset) : ?PageHeader
    {
        $currentOffset = \ftell($stream);

        try {
            \fseek($stream, $pageOffset);
            $thriftHeader = new \Flow\Parquet\Thrift\PageHeader();
            @$thriftHeader->read(new TCompactProtocol(new TBufferedTransport(new TPhpFileStream($stream))));

            if ($thriftHeader->type === null) {
                return null;
            }

            return PageHeader::fromThrift($thriftHeader);
        } catch (\Throwable $e) {
            /** @phpstan-ignore-next-line */
            \fseek($stream, $currentOffset);

            return null;
        }
    }
}
