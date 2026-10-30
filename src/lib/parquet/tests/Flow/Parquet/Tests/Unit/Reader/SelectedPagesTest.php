<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\SelectedPages;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_map;
use function iterator_to_array;

final class SelectedPagesTest extends TestCase
{
    public function test_only_the_pages_holding_a_selected_row_are_yielded_with_their_rows(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footer = (new PhpParquetFileReader(
            new MemorySourceStream($bytes),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
        $indexes = new StreamIndexes($footer, new MemorySourceStream($bytes));
        $id = $footer->metadata()->schema()->getFlat('id');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $offsetIndex = $indexes->offsetIndex(0, $id) ?? static::fail('No offset index');

        $pages = iterator_to_array((new SelectedPages(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
        ))->read(
            $id,
            $footer->metadata()->rowGroups()->all()[0],
            0,
            $stream,
            RowSelection::window(12, 3, 2),
            $indexes,
            ['pages' => [], 'dictionary' => null],
        ));

        static::assertSame(
            [2 => [[2, 3], 1], 4 => [[4, 5], 1]],
            array_map(static fn(array $page): array => [
                $page[0]->rawValues(),
                $page[1]->rowCount(),
            ], $pages),
        );
        static::assertSame(
            $offsetIndex->pages[1]->compressedPageSize + $offsetIndex->pages[2]->compressedPageSize,
            $stream->bytes,
        );
    }

    public function test_trimmed_pages_hold_only_the_selected_rows(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footer = (new PhpParquetFileReader(
            new MemorySourceStream($bytes),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
        $stream = new MemorySourceStream($bytes);

        static::assertSame(
            [[3], [4]],
            array_map(
                static fn(ReadFlatColumnValues $page): array => $page->rawValues(),
                iterator_to_array((new SelectedPages(
                    new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
                ))->trimmed(
                    $footer->metadata()->schema()->getFlat('id'),
                    $footer->metadata()->rowGroups()->all()[0],
                    0,
                    $stream,
                    RowSelection::window(12, 3, 2),
                    new StreamIndexes($footer, $stream),
                    ['pages' => [], 'dictionary' => null],
                ), false),
            ),
        );
    }

    public function test_a_decoded_page_is_served_and_only_the_other_selected_page_read(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footer = (new PhpParquetFileReader(
            new MemorySourceStream($bytes),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
        $indexes = new StreamIndexes($footer, new MemorySourceStream($bytes));
        $id = $footer->metadata()->schema()->getFlat('id');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        static::assertSame(
            [[50], [6]],
            array_map(
                static fn(ReadFlatColumnValues $page): array => $page->rawValues(),
                iterator_to_array((new SelectedPages(
                    new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
                ))->trimmed(
                    $id,
                    $footer->metadata()->rowGroups()->all()[0],
                    0,
                    $stream,
                    RowSelection::window(12, 5, 2),
                    $indexes,
                    ['pages' => [4 => new ReadFlatColumnValues($id, [40, 50], [0, 0], [1, 1])], 'dictionary' => null],
                ), false),
            ),
        );
        static::assertSame(
            ($indexes->offsetIndex(0, $id) ?? static::fail('No offset index'))->pages[3]->compressedPageSize,
            $stream->bytes,
        );
    }

    public function test_when_every_selected_page_is_decoded_nothing_is_read_and_the_given_dictionary_returned(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footer = (new PhpParquetFileReader(
            new MemorySourceStream($bytes),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        $pages = (new SelectedPages(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
        ))->read(
            $footer->metadata()->schema()->getFlat('word'),
            $footer->metadata()->rowGroups()->all()[0],
            0,
            $stream,
            RowSelection::window(12, 2, 2),
            new StreamIndexes($footer, new MemorySourceStream($bytes)),
            [
                'pages' => [2 => new ReadFlatColumnValues(
                    $footer->metadata()->schema()->getFlat('word'),
                    ['a', 'b'],
                    [0, 0],
                    [1, 1],
                )],
                'dictionary' => null,
            ],
        );

        static::assertSame([2], array_keys(iterator_to_array($pages)));
        static::assertNull($pages->getReturn());
        static::assertSame(0, $stream->reads);
    }

    public function test_without_an_offset_index_the_decoded_pages_are_served_alone(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $metadata = (new PhpParquetFileReader(new MemorySourceStream($bytes), ByteOrder::LITTLE_ENDIAN, new Options()))
            ->footer()
            ->metadata();
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));

        static::assertSame(
            [[40, 50], [60]],
            array_map(
                static fn(ReadFlatColumnValues $page): array => $page->rawValues(),
                iterator_to_array((new SelectedPages(
                    new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
                ))->trimmed(
                    $metadata->schema()->getFlat('id'),
                    $metadata->rowGroups()->all()[0],
                    0,
                    $stream,
                    RowSelection::window(12, 4, 3),
                    new InMemoryIndexes(),
                    [
                        'pages' => [
                            4 => new ReadFlatColumnValues($metadata->schema()->getFlat('id'), [40, 50], [0, 0], [1, 1]),
                            6 => new ReadFlatColumnValues($metadata->schema()->getFlat('id'), [60, 70], [0, 0], [1, 1]),
                        ],
                        'dictionary' => null,
                    ],
                ), false),
            ),
        );
        static::assertSame(0, $stream->reads);
    }

    public function test_a_row_group_decoding_more_rows_than_its_selection_is_refused(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Row group 0 decodes more than the 1 rows its metadata declares');

        iterator_to_array((new SelectedPages(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
        ))->read(
            $metadata->schema()->getFlat('id'),
            $metadata->rowGroups()->all()[0],
            0,
            $stream,
            RowSelection::all(1),
            new InMemoryIndexes(),
            ['pages' => [], 'dictionary' => null],
        ));
    }
}
