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
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_map;
use function iterator_to_array;

final class ColumnChunkReaderTest extends TestCase
{
    public function test_the_given_pages_are_read_in_one_call(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $id = $file->schema()->getFlat('id');
        $offsetIndex = $file->indexes()->offsetIndex(0, $id) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read($file->metadata()->rowGroups()->all()[0]->getColumnChunk($id), $id, $stream, $offsetIndex, [2, 3]));

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

    public function test_the_dictionary_page_is_read_beside_the_given_pages(): void
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
        ))->read($chunk, $word, $stream, $offsetIndex, [3]));

        static::assertSame(
            [6 => ['w0', 'w1']],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
        static::assertSame(2, $stream->reads);
        static::assertSame(
            $chunk->dataPageOffset() - $chunk->pageOffset() + $offsetIndex->pages[3]->compressedPageSize,
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
        $first = $reader->read($chunk, $word, new MemorySourceStream($bytes), $offsetIndex, [0]);
        iterator_to_array($first);
        $dictionary = $first->getReturn() ?? static::fail('No dictionary');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = iterator_to_array($reader->read($chunk, $word, $stream, $offsetIndex, [3], $dictionary));

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
        ))->read($chunk, $word, $stream, $offsetIndex, [0, 1]));

        static::assertSame(1, $stream->reads);
        static::assertSame($offsetIndex->pages[2]->offset - $chunk->pageOffset(), $stream->bytes);
    }

    public function test_a_dictionary_the_data_page_offset_points_at_is_read_beside_the_given_pages(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word);
        $offsetIndex = $file->indexes()->offsetIndex(0, $word) ?? static::fail('No offset index');

        $pages = iterator_to_array((new ColumnChunkReader(
            new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()),
            new Options(),
        ))->read(ColumnChunkMother::parquet816($chunk), $word, new MemorySourceStream($bytes), $offsetIndex, [3]));

        static::assertSame(
            [6 => ['w0', 'w1']],
            array_map(static fn(ReadFlatColumnValues $page): array => iterator_to_array(
                $page->rawValues(),
                false,
            ), $pages),
        );
    }

    public function test_no_pages_read_nothing_and_return_the_given_dictionary(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $word = $file->schema()->getFlat('word');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = (new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()))->read(
            $file->metadata()->rowGroups()->all()[0]->getColumnChunk($word),
            $word,
            $stream,
            $file->indexes()->offsetIndex(0, $word),
            [],
        );

        static::assertSame([], iterator_to_array($pages));
        static::assertNull($pages->getReturn());
        static::assertSame(0, $stream->reads);
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
        ))->read($chunk, $id, $stream));

        static::assertSame([0, 2, 4, 6, 8, 10], array_keys($pages));
        static::assertSame(1, $stream->reads);
        static::assertSame($chunk->totalCompressedSize(), $stream->bytes);
    }
}
