<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_map;
use function iterator_to_array;

final class ColumnChunkReaderTest extends TestCase
{
    public function test_an_offset_index_reads_only_the_pages_holding_a_selected_row(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $id = $file->schema()->getFlat('id');
        $offsetIndex = $file->indexes()->offsetIndex(0, $id) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read(
            $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id),
            $id,
            $stream,
            $offsetIndex,
            RowSelection::window(12, 5, 2),
        ));

        static::assertSame(
            [4 => [4, 5], 6 => [6, 7]],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame(1, $stream->reads);
        static::assertSame(
            $offsetIndex->pages[2]->compressedPageSize + $offsetIndex->pages[3]->compressedPageSize,
            $stream->bytes,
        );
    }

    public function test_the_dictionary_page_is_read_beside_the_selected_pages(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word);
        $offsetIndex = $file->indexes()->offsetIndex(0, $word) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read($chunk, $word, $stream, $offsetIndex, RowSelection::window(12, 6, 2)));

        static::assertSame(
            [6 => ['w0', 'w1']],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame(2, $stream->reads);
        static::assertSame(
            $offsetIndex->pages[0]->offset - $chunk->pageOffset() + $offsetIndex->pages[3]->compressedPageSize,
            $stream->bytes,
        );
    }

    public function test_a_given_dictionary_is_used_instead_of_read(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word);
        $offsetIndex = $file->indexes()->offsetIndex(0, $word) ?? static::fail('No offset index');
        $reader = new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options());
        $first = $reader->read(
            $chunk,
            $word,
            new MemorySourceStream($bytes),
            $offsetIndex,
            RowSelection::window(12, 0, 2),
        );
        iterator_to_array($first);
        $dictionary = $first->getReturn() ?? static::fail('No dictionary');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array($reader->read(
            $chunk,
            $word,
            $stream,
            $offsetIndex,
            RowSelection::window(12, 6, 2),
            [],
            $dictionary,
        ));

        static::assertSame(['w0', 'w1', 'w2'], $dictionary->values);
        static::assertSame(
            [6 => ['w0', 'w1']],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame($offsetIndex->pages[3]->compressedPageSize, $stream->bytes);
    }

    public function test_adjacent_pages_and_the_dictionary_are_read_in_one_call(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word);
        $offsetIndex = $file->indexes()->offsetIndex(0, $word) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read($chunk, $word, $stream, $offsetIndex, RowSelection::window(12, 1, 3)));

        static::assertSame(1, $stream->reads);
        static::assertSame($offsetIndex->pages[2]->offset - $chunk->pageOffset(), $stream->bytes);
    }

    public function test_without_an_offset_index_the_whole_chunk_is_read_once(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $id = $file->schema()->getFlat('id');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id);
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read($chunk, $id, $stream, null, RowSelection::window(12, 5, 2)));

        static::assertSame([0, 2, 4, 6, 8, 10], array_keys($pages));
        static::assertSame(1, $stream->reads);
        static::assertSame($chunk->totalCompressedSize(), $stream->bytes);
    }

    public function test_a_decoded_page_is_served_instead_of_read(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $id = $file->schema()->getFlat('id');
        $offsetIndex = $file->indexes()->offsetIndex(0, $id) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read(
            $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id),
            $id,
            $stream,
            $offsetIndex,
            RowSelection::window(12, 5, 2),
            [4 => ['values' => [40, 50], 'repetitionLevels' => [0, 0], 'definitionLevels' => [1, 1]]],
        ));

        static::assertSame(
            [4 => [40, 50], 6 => [6, 7]],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame($offsetIndex->pages[3]->compressedPageSize, $stream->bytes);
    }

    public function test_when_every_selected_page_is_decoded_nothing_is_read_not_even_the_dictionary(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read(
            $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word),
            $word,
            $stream,
            $file->indexes()->offsetIndex(0, $word),
            RowSelection::window(12, 2, 2),
            [2 => ['values' => ['a', 'b'], 'repetitionLevels' => [0, 0], 'definitionLevels' => [1, 1]]],
        ));

        static::assertSame([2], array_keys($pages));
        static::assertSame(0, $stream->reads);
    }

    public function test_without_an_offset_index_the_decoded_pages_are_served_alone(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $id = $file->schema()->getFlat('id');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read(
            $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id),
            $id,
            $stream,
            null,
            RowSelection::window(12, 4, 3),
            [
                4 => ['values' => [40, 50], 'repetitionLevels' => [0, 0], 'definitionLevels' => [1, 1]],
                6 => ['values' => [60, 70], 'repetitionLevels' => [0, 0], 'definitionLevels' => [1, 1]],
            ],
        ));

        static::assertSame(
            [4 => [40, 50], 6 => [60, 70]],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame(0, $stream->reads);
    }
}
