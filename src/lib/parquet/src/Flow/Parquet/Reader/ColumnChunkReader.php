<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\{PageHeader};
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftStream\{TPhpFileStream};
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\{TBufferedTransport};

final readonly class ColumnChunkReader
{
    public function __construct(
        private PageReader $pageReader,
        private Options $options,
    ) {
    }

    /**
     * @return \Generator<ReadFlatColumnValues>
     */
    public function read(ColumnChunk $columnChunk, FlatColumn $column, SourceStream $stream) : \Generator
    {
        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        /** @phpstan-ignore-next-line */
        \fwrite($pageStream, $stream->read($columnChunk->totalCompressedSize(), $columnChunk->pageOffset()));
        \rewind($pageStream);

        $header = $this->readHeader($pageStream);

        if ($header === null) {
            throw new RuntimeException('Cannot read first page header');
        }

        if ($header->type()->isDictionaryPage()) {
            $dictionary = $this->pageReader->readDictionary(
                $column,
                $header,
                $columnChunk->codec(),
                $pageStream
            );
        } else {
            $dictionary = null;
        }

        $rowsToRead = $columnChunk->valuesCount();

        $yieldedRows = 0;

        while (true) {
            $dataHeader = $dictionary ? $this->readHeader($pageStream) : $header;

            if ($dataHeader === null || $dataHeader->type()->isDataPage() === false) {
                break;
            }

            if ($yieldedRows >= $rowsToRead) {
                break;
            }

            $data = $this->pageReader->readData(
                $column,
                $dataHeader,
                $columnChunk->codec(),
                $dictionary,
                $pageStream
            );

            $yieldedRows += $data->rowsCount();

            yield $data;

            if ($dictionary === null) {
                $header = $this->readHeader($pageStream);
            }
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
