<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use DateTimeImmutable;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\ColumnReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function iterator_to_array;

final class ColumnReaderTest extends TestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([0, [1, 2]])]
    #[TestWith([1, [3, 4]])]
    #[TestWith([2, [5, 6]])]
    public function test_a_flat_column_reads_the_rows_of_one_row_group(int $group, array $ids): void
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
                $reader->readRowGroup(
                    $metadata->schema()->get('id'),
                    $metadata->rowGroups()->all()[$group],
                    $group,
                    $stream,
                    RowSelection::all(2),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
    }

    /**
     * @param list<int> $ids
     */
    #[TestWith([1, 0, [3]])]
    #[TestWith([0, 1, [4]])]
    #[TestWith([1, 1, []])]
    public function test_a_flat_column_reads_only_the_selected_rows_of_a_row_group(
        int $skipped,
        int $offset,
        array $ids,
    ): void {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            array_map(static fn(int $id): array => ['id' => $id], $ids),
            iterator_to_array(
                $reader->readRowGroup(
                    $metadata->schema()->get('id'),
                    $metadata->rowGroups()->all()[1],
                    1,
                    $stream,
                    RowSelection::window(2, $offset, 2 - $offset - $skipped),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
    }

    public function test_a_nested_column_reads_the_rows_of_one_row_group(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            [['tags' => ['t4']]],
            iterator_to_array(
                $reader->readRowGroup(
                    $metadata->schema()->get('tags'),
                    $metadata->rowGroups()->all()[1],
                    1,
                    $stream,
                    RowSelection::window(2, 1),
                    new InMemoryIndexes(),
                ),
                false,
            ),
        );
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

        iterator_to_array($reader->readRowGroup(
            $metadata->schema()->get('id'),
            $metadata->rowGroups()->all()[0],
            0,
            $stream,
            RowSelection::all(1),
            new InMemoryIndexes(),
        ));
    }

    public function test_a_nested_leaf_keeps_only_the_masked_rows_inside_a_page(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::pagedColumns());
        $footer = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer();
        $metadata = $footer->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );
        $selection = RowSelection::window(12, 2, 5)->masked([false, true, false, true, false]);

        static::assertSame(
            [['tags' => [3]], ['tags' => [5, 5, 5]]],
            iterator_to_array(
                $reader->readRowGroup(
                    $metadata->schema()->get('tags'),
                    $metadata->rowGroups()->all()[0],
                    0,
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
                $reader->readRowGroup(
                    $metadata->schema()->get('id'),
                    $metadata->rowGroups()->all()[0],
                    0,
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
                $reader->readRowGroup(
                    $metadata->schema()->get('id'),
                    $metadata->rowGroups()->all()[0],
                    0,
                    $stream,
                    RowSelection::window(12, 5, 2),
                    new InMemoryIndexes([], ['id' => [PageIndexMother::pagesAt(0, 6, 2)]]),
                ),
                false,
            ),
        );
    }

    public function test_physical_values_are_undecoded_and_null_where_the_leaf_or_its_struct_is_null_beside_the_pages_decoded(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::written(
            Writer::php(),
            Schema::with(FlatColumn::date('day'), NestedColumn::struct('address', [FlatColumn::string('city')])),
            [
                ['day' => new DateTimeImmutable('1970-01-02 UTC'), 'address' => ['city' => 'a']],
                ['day' => null, 'address' => null],
                ['day' => new DateTimeImmutable('1970-01-04 UTC'), 'address' => ['city' => null]],
                ['day' => new DateTimeImmutable('1970-01-05 UTC'), 'address' => ['city' => 'b']],
            ],
        ));
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );
        $selection = RowSelection::window(4, 0, 3);
        $day = $reader->readPhysical(
            $metadata->schema()->getFlat('day'),
            $metadata->rowGroups()->all()[0],
            0,
            $stream,
            $selection,
            new InMemoryIndexes(),
        );

        static::assertSame(
            [
                0 => [
                    'values' => [1, 3, 4],
                    'repetitionLevels' => [0, 0, 0, 0],
                    'definitionLevels' => [1, 0, 1, 1],
                ],
            ],
            $day['chunk']['pages'],
        );
        static::assertSame([1, null, 3], $day['values']);
        static::assertSame(
            ['a', null, null],
            $reader->readPhysical(
                $metadata->schema()->getFlat('address.city'),
                $metadata->rowGroups()->all()[0],
                0,
                $stream,
                $selection,
                new InMemoryIndexes(),
            )['values'],
        );
        static::assertSame(
            ['chunk' => ['pages' => [], 'dictionary' => null], 'values' => []],
            $reader->readPhysical(
                $metadata->schema()->getFlat('day'),
                $metadata->rowGroups()->all()[0],
                0,
                $stream,
                RowSelection::none(4),
                new InMemoryIndexes(),
            ),
        );
    }

    public function test_the_dictionary_read_for_the_physical_values_is_not_read_again_for_the_output(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footer = (new PhpParquetFileReader(
            new MemorySourceStream($bytes),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->footer();
        $indexes = new StreamIndexes($footer, new MemorySourceStream($bytes));
        $word = $footer->metadata()->schema()->getFlat('word');
        $rowGroup = $footer->metadata()->rowGroups()->all()[0];
        $offsetIndex = $indexes->offsetIndex(0, $word) ?? static::fail('No offset index');
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        $physical = $reader->readPhysical($word, $rowGroup, 0, $stream, RowSelection::window(12, 6, 2), $indexes);
        $read = $stream->bytes;

        static::assertSame(
            [['word' => 'w0'], ['word' => 'w1'], ['word' => 'w2'], ['word' => 'w0']],
            iterator_to_array($reader->readRowGroup(
                $word,
                $rowGroup,
                0,
                $stream,
                RowSelection::window(12, 6, 4),
                $indexes,
                ['word' => $physical['chunk']],
            ), false),
        );
        static::assertSame($offsetIndex->pages[4]->compressedPageSize, $stream->bytes - $read);
    }

    public function test_physical_values_of_a_repeated_leaf_are_refused(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->footer()->metadata();
        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column "tags.list.element" lies inside a list or a map, where a row holds no single value',
        );

        $reader->readPhysical(
            $metadata->schema()->getFlat('tags.list.element'),
            $metadata->rowGroups()->all()[0],
            0,
            $stream,
            RowSelection::all(2),
            new InMemoryIndexes(),
        );
    }
}
