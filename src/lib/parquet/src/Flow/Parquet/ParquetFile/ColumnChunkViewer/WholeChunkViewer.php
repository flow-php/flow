<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\ColumnChunkViewer;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\ColumnChunkViewer;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TBufferedTransport;

final readonly class WholeChunkViewer implements ColumnChunkViewer
{
    public function __construct(private Options $options)
    {
    }

    public function view(ColumnChunk $columnChunk, FlatColumn $column, SourceStream $stream) : \Generator
    {
        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        /** @phpstan-ignore-next-line */
        \fwrite($pageStream, $stream->read($columnChunk->totalCompressedSize(), $columnChunk->pageOffset()));
        \rewind($pageStream);

        if ($columnChunk->dictionaryPageOffset()) {
            $dictionaryHeader = $this->readHeader($pageStream);

            if ($dictionaryHeader === null) {
                throw new RuntimeException('Dictionary page header not found in column chunk under offset: ' . $columnChunk->pageOffset());
            }

            yield $dictionaryHeader;
        }

        while (true) {
            $dataHeader = $this->readHeader($pageStream);

            /** There are no more pages in given column chunk */
            if ($dataHeader === null || $dataHeader->type()->isDataPage() === false) {
                break;
            }

            \fseek($pageStream, \ftell($pageStream) + $dataHeader->compressedPageSize());

            yield $dataHeader;
        }

        \fclose($pageStream);
    }

    /**
     * @param resource $stream
     */
    private function readHeader($stream) : ?PageHeader
    {
        $currentOffset = \ftell($stream);

        try {
            $thriftHeader = new \Flow\Parquet\Thrift\PageHeader();
            @$thriftHeader->read(new TCompactProtocol(new TBufferedTransport(new TPhpFileStream($stream))));

            if ($thriftHeader->type === null) {
                return null;
            }

            return PageHeader::fromThrift($thriftHeader, $this->options);
        } catch (\Throwable) {
            /** @phpstan-ignore-next-line */
            \fseek($stream, $currentOffset);

            return null;
        }
    }
}
