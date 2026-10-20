<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Generator;

use function array_key_last;
use function array_map;
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
     * The chunk's data pages keyed by the row of the row group each starts at: the $pages of $offsetIndex, one
     * SourceStream::read per run of adjacent pages, or every page without an offset index. The dictionary is read from
     * the chunk's start up to the first indexed page, wherever `dictionary_page_offset` points (PARQUET-816), unless
     * $dictionary is given. Returns the dictionary the pages were decoded with.
     *
     * @param list<int> $pages ascending positions in $offsetIndex
     *
     * @return Generator<int, ReadFlatColumnValues, mixed, ?Dictionary>
     */
    public function read(
        ColumnChunk $columnChunk,
        FlatColumn $column,
        SourceStream $stream,
        ?OffsetIndex $offsetIndex = null,
        array $pages = [],
        ?Dictionary $dictionary = null,
    ): Generator {
        if ($offsetIndex === null) {
            return yield from $this->readChunk($columnChunk, $column, $stream);
        }

        if ($pages === []) {
            return $dictionary;
        }

        $firstPage = $offsetIndex->pages[0]->offset;
        $leadingRegion = $dictionary === null && $columnChunk->pageOffset() < $firstPage;
        $runs = $leadingRegion ? [new PageRun($columnChunk->pageOffset(), $firstPage)] : [];
        $locations = array_map(static fn(int $page): PageLocation => $offsetIndex->pages[$page], $pages);

        foreach ($locations as $page) {
            $end = $page->offset + $page->compressedPageSize;
            $last = array_key_last($runs);

            if ($last !== null && $runs[$last]->end === $page->offset) {
                $runs[$last] = $runs[$last]->extendedTo($end);
            } else {
                $runs[] = new PageRun($page->offset, $end);
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

        if ($leadingRegion) {
            rewind($pageStream);
            $header = PageHeader::read($pageStream, $this->options);

            if ($header !== null && $header->type()->isDictionaryPage()) {
                $dictionary = $this->pageReader->readDictionary($column, $header, $columnChunk->codec(), $pageStream);
            }
        }

        $run = 0;

        foreach ($locations as $page) {
            while ($runs[$run]->end <= $page->offset) {
                $run++;
            }

            fseek($pageStream, $buffered[$run] + $page->offset - $runs[$run]->start);
            $header = PageHeader::read($pageStream, $this->options);

            if ($header === null || !$header->type()->isDataPage()) {
                throw new RuntimeException(
                    'No data page header at offset ' . $page->offset . ' the offset index locates',
                );
            }

            yield $page->firstRowIndex => $this->pageReader->readData(
                $column,
                $header,
                $columnChunk->codec(),
                $dictionary,
                $pageStream,
            );
        }

        fclose($pageStream);

        return $dictionary;
    }

    /**
     * @return Generator<int, ReadFlatColumnValues, mixed, ?Dictionary>
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

        $header = PageHeader::read($pageStream, $this->options);

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
            $dataHeader = $dictionary ? PageHeader::read($pageStream, $this->options) : $header;

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
                $header = PageHeader::read($pageStream, $this->options);
            }
        }

        fclose($pageStream);

        return $dictionary;
    }
}
