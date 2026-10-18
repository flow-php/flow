<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\PhpFileStream;
use Flow\Parquet\ThriftModel\PageHeader as ThriftPageHeader;
use Generator;
use Throwable;

use function array_key_last;
use function array_keys;
use function fclose;
use function fopen;
use function fseek;
use function ftell;
use function fwrite;
use function rewind;

use const SEEK_END;

final readonly class ColumnChunkReader
{
    public function __construct(
        private PageReader $pageReader,
        private Options $options,
    ) {}

    /**
     * The chunk's data pages keyed by the row of the row group each starts at. With an offset index only the
     * dictionary page and the pages holding a row $rowGroupSelection selects are read, one SourceStream::read per run
     * of adjacent pages; without one the whole chunk is read.
     *
     * @return Generator<int, ReadFlatColumnValues>
     */
    public function read(
        ColumnChunk $columnChunk,
        FlatColumn $column,
        SourceStream $stream,
        ?OffsetIndex $offsetIndex,
        RowSelection $rowGroupSelection,
    ): Generator {
        if ($offsetIndex === null || $offsetIndex->pages === []) {
            yield from $this->readChunk($columnChunk, $column, $stream);

            return;
        }

        $pages = $offsetIndex->pages;
        $pageRows = [];

        foreach (array_keys($pages) as $page) {
            $pageRows[] = $offsetIndex->pageRows($page, $rowGroupSelection->totalRows());
        }

        $leadingRegion = $columnChunk->pageOffset() < $pages[0]->offset;
        $runs = $leadingRegion ? [new PageRun($columnChunk->pageOffset(), $pages[0]->offset)] : [];
        $selected = [];

        foreach ($rowGroupSelection->split($pageRows) as $page => $pageSelection) {
            if (!$pageSelection->selectsAny()) {
                continue;
            }

            $selected[] = $page;
            $start = $pages[$page]->offset;
            $end = $start + $pages[$page]->compressedPageSize;
            $last = array_key_last($runs);

            if ($last !== null && $runs[$last]->end === $start) {
                $runs[$last] = $runs[$last]->extendedTo($end);
            } else {
                $runs[] = new PageRun($start, $end);
            }
        }

        $pageStream = fopen('php://temp', 'rb+');

        if ($pageStream === false) {
            throw new RuntimeException('Cannot open temporary stream');
        }

        $buffered = [];
        $read = 0;

        foreach ($runs as $pageRun) {
            $buffered[] = $read;
            $read += (int) fwrite($pageStream, $stream->read($pageRun->length, $pageRun->start));
        }

        $dictionary = null;

        if ($leadingRegion) {
            rewind($pageStream);
            $header = $this->readHeader($pageStream);

            if ($header !== null && $header->type()->isDictionaryPage()) {
                $dictionary = $this->pageReader->readDictionary($column, $header, $columnChunk->codec(), $pageStream);
            }
        }

        $run = 0;

        foreach ($selected as $page) {
            while ($runs[$run]->end <= $pages[$page]->offset) {
                $run++;
            }

            fseek($pageStream, $buffered[$run] + $pages[$page]->offset - $runs[$run]->start);
            $header = $this->readHeader($pageStream);

            if ($header === null || !$header->type()->isDataPage()) {
                throw new RuntimeException(
                    'No data page header at offset ' . $pages[$page]->offset . ' the offset index locates',
                );
            }

            yield $pages[$page]->firstRowIndex => $this->pageReader->readData(
                $column,
                $header,
                $columnChunk->codec(),
                $dictionary,
                $pageStream,
            );
        }

        fclose($pageStream);
    }

    /**
     * @return Generator<int, ReadFlatColumnValues>
     */
    private function readChunk(ColumnChunk $columnChunk, FlatColumn $column, SourceStream $stream): Generator
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

        $header = $this->readHeader($pageStream);

        if ($header === null) {
            throw new RuntimeException('Cannot read first page header');
        }

        if ($header->type()->isDictionaryPage()) {
            // PARQUET-816: old parquet-mr writers set data_page_offset to the dictionary page
            // and left dictionary_page_offset null, excluding the dictionary page header size
            // from total_compressed_size. Extend the buffer with the missing bytes.
            if ($columnChunk->dictionaryPageOffset() === null) {
                /** @var int<1, max> $dictHeaderSize */
                $dictHeaderSize = (int) ftell($pageStream);
                fseek($pageStream, 0, SEEK_END);
                fwrite($pageStream, $stream->read(
                    $dictHeaderSize,
                    $columnChunk->pageOffset() + $columnChunk->totalCompressedSize(),
                ));
                fseek($pageStream, $dictHeaderSize);
            }

            $dictionary = $this->pageReader->readDictionary($column, $header, $columnChunk->codec(), $pageStream);
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

            $data = $this->pageReader->readData($column, $dataHeader, $columnChunk->codec(), $dictionary, $pageStream);

            yield $yieldedRows => $data;

            $yieldedRows += $data->rowsCount();

            if ($dictionary === null) {
                $header = $this->readHeader($pageStream);
            }
        }

        fclose($pageStream);
    }

    /**
     * @param resource $stream
     */
    private function readHeader($stream): ?PageHeader
    {
        $currentOffset = ftell($stream);

        try {
            $thriftHeader = new ThriftPageHeader();
            @$thriftHeader->read(new CompactProtocol(new PhpFileStream($stream)));

            return PageHeader::fromThrift($thriftHeader, $this->options);
        } catch (Throwable) {
            if ($currentOffset !== false) {
                fseek($stream, $currentOffset);
            }

            return null;
        }
    }
}
