<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\ColumnReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function iterator_to_array;

final class ColumnReaderTest extends TestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([null, 0, [1, 2, 3, 4, 5, 6]])]
    #[TestWith([null, 3, [4, 5, 6]])]
    #[TestWith([2, 1, [2, 3]])]
    #[TestWith([3, 2, [3, 4, 5]])]
    #[TestWith([1, 5, [6]])]
    #[TestWith([null, 6, []])]
    public function test_a_flat_column_reads_a_window_across_row_groups(?int $limit, int $offset, array $ids): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();

        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            array_map(static fn(int $id): array => ['id' => $id], $ids),
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('id'),
                    $metadata,
                    $stream,
                    RowSelection::window(6, $offset, $limit)->split([2, 2, 2]),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
    }

    public function test_a_flat_column_reads_only_the_selected_rows_of_each_row_group(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();

        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            [['id' => 2], ['id' => 5], ['id' => 6]],
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('id'),
                    $metadata,
                    $stream,
                    (new RowSelection(
                        new RowSelector(1, true),
                        new RowSelector(1, false),
                        new RowSelector(2, true),
                        new RowSelector(2, false),
                    ))->split([2, 2, 2]),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
    }

    public function test_a_nested_column_reads_across_row_groups(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();

        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            array_map(static fn(int $id): array => ['tags' => ['t' . $id]], [2, 3, 4]),
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('tags'),
                    $metadata,
                    $stream,
                    RowSelection::window(6, 1, 3)->split([2, 2, 2]),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
    }

    public function test_a_selection_of_another_row_group_count_is_refused(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A selection of 2 row groups cannot read a file of 3 row groups');

        iterator_to_array($reader->read(
            $metadata->schema()->get('id'),
            $metadata,
            $stream,
            RowSelection::all(6)->split([3, 3]),
            new InMemoryIndexes(),
        ));
    }

    public function test_a_row_group_decoding_more_rows_than_its_selection_is_refused(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Row group 0 decodes more than the 1 rows its metadata declares');

        iterator_to_array($reader->read(
            $metadata->schema()->get('id'),
            $metadata,
            $stream,
            [
                RowSelection::all(1),
                RowSelection::all(2),
                RowSelection::all(2),
            ],
            new InMemoryIndexes(),
        ));
    }

    public function test_a_nested_leaf_keeps_only_the_selected_rows_inside_a_page(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::pagedColumns());
        $footer = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer();
        $metadata = $footer->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );
        $selection = [new RowSelection(
            new RowSelector(3, true),
            new RowSelector(1, false),
            new RowSelector(1, true),
            new RowSelector(1, false),
            new RowSelector(6, true),
        )];

        static::assertSame(
            [['tags' => [3]], ['tags' => [5, 5, 5]]],
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('tags'),
                    $metadata,
                    $stream,
                    $selection,
                    new StreamIndexes($footer, $stream),
                ),
                false,
            ),
        );
        static::assertSame(
            [['id' => 3], ['id' => 5]],
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('id'),
                    $metadata,
                    $stream,
                    $selection,
                    new StreamIndexes($footer, $stream),
                ),
                false,
            ),
        );
    }

    public function test_an_offset_index_that_does_not_split_the_group_is_not_used(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::pagedColumns());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            [['id' => 5], ['id' => 6]],
            iterator_to_array(
                $reader->read(
                    $metadata->schema()->get('id'),
                    $metadata,
                    $stream,
                    [RowSelection::window(12, 5, 2)],
                    new InMemoryIndexes([], ['id' => [PageIndexMother::pagesAt(0, 6, 2)]]),
                ),
                false,
            ),
        );
    }
}
