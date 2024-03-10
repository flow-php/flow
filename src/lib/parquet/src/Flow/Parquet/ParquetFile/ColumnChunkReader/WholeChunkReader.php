<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\ColumnChunkReader;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\DataBuilder;
use Flow\Parquet\ParquetFile\Page\{ColumnData, PageHeader};
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\{ColumnChunkReader, PageReader};
use Flow\Parquet\ThriftStream\TPhpFileStream;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TBufferedTransport;

final class WholeChunkReader implements ColumnChunkReader
{
    public function __construct(
        private readonly DataBuilder $dataBuilder,
        private readonly PageReader $pageReader,
    ) {
    }

    /**
     * @param resource $stream
     *
     * @return \Generator<array<mixed>>
     */
    public function read(ColumnChunk $columnChunk, FlatColumn $column, $stream) : \Generator
    {
        $offset = $columnChunk->pageOffset();

        \fseek($stream, $offset);

        $firstHeader = $this->readHeader($stream, $offset);

        if ($firstHeader === null) {
            throw new RuntimeException('Cannot read first page header');
        }

        if ($firstHeader->type()->isDictionaryPage()) {
            $dictionary = $this->pageReader->readDictionary(
                $column,
                $firstHeader,
                $columnChunk->codec(),
                $stream
            );
            $offset = \ftell($stream);
        } else {
            $dictionary = null;
            \fseek($stream, $offset);
        }

        $columnData = ColumnData::initialize($column);

        $rowsToRead = $columnChunk->valuesCount();

        while (true) {
            /** @phpstan-ignore-next-line */
            $dataHeader = $this->readHeader($stream, $offset);

            /** There are no more pages in given column chunk */
            if ($dataHeader === null || $columnData->size() >= $rowsToRead || $dataHeader->type()->isDataPage() === false) {
                $yieldedRows = 0;

                /** @var array $row */
                foreach ($this->dataBuilder->build($columnData, $column) as $row) {
                    yield $row;
                    $yieldedRows++;

                    if ($yieldedRows >= $rowsToRead) {
                        return;
                    }
                }

                break;
            }

            $columnData = $columnData->merge($this->pageReader->readData(
                $column,
                $dataHeader,
                $columnChunk->codec(),
                $dictionary,
                $stream
            ));

            $offset = \ftell($stream);
        }
    }

    /**
     * @param resource $stream
     *
     * @psalm-suppress DocblockTypeContradiction
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
