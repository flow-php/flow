<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Double\SpyParquetFileReader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_map;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_query;
use function iterator_to_array;
use function range;

final class ParquetFileTest extends TestCase
{
    public function test_close_closes_the_reader_once(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')));
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        $file->close();
        $file->close();
        unset($file);

        static::assertSame(1, $reader->closes);
    }

    public function test_dropping_the_file_closes_the_reader(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')));
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        unset($file);

        static::assertSame(1, $reader->closes);
    }

    public function test_metadata_schema_and_reader_come_from_the_reader(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));
        $reader = new SpyParquetFileReader($schema);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame($reader, $file->reader());
        static::assertSame($schema, $file->schema());
        static::assertSame($schema, $file->metadata()->schema());
    }

    public function test_options_are_the_ones_the_file_was_opened_with(): void
    {
        $options = Options::default()->set(Option::INT_96_AS_DATETIME, false);

        static::assertSame(
            $options,
            (new ParquetFile(
                new MemorySourceStream('PAR1'),
                $options,
                new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'))),
            ))->options(),
        );
    }

    public function test_columns_reads_the_selected_columns_of_the_window_through_the_reader(): void
    {
        $reader = new SpyParquetFileReader(
            Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
            [[
                'name' => ['a'],
                'id' => [1],
            ]],
            rows: 10,
        );
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['name' => ['a'], 'id' => [1]]],
            iterator_to_array($file->columns(parquet_query()->select('name', 'id')->offset(2)->limit(5), 10), false),
        );
        static::assertEquals([[['name', 'id'], 10, ReadPlan::window(10, $file->indexes(), 2, 5)]], $reader->reads);
    }

    public function test_columns_without_a_selection_reads_every_root_column(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')), rows: 4);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        iterator_to_array($file->columns());

        static::assertEquals([[['id', 'name'], 1024, ReadPlan::window(4, $file->indexes())]], $reader->reads);
    }

    public function test_columns_refuses_a_column_the_schema_lacks(): void
    {
        $file = new ParquetFile(
            new MemorySourceStream('PAR1'),
            new Options(),
            new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'))),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "missing" does not exist');

        iterator_to_array($file->columns(parquet_query()->select('missing')));
    }

    public function test_columns_refuses_a_batch_size_below_one(): void
    {
        $file = new ParquetFile(
            new MemorySourceStream('PAR1'),
            new Options(),
            new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'))),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');

        iterator_to_array($file->columns(batchSize: 0));
    }

    public function test_a_filtered_read_returns_only_the_matching_rows_of_the_selected_columns(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')), [
            ['id' => [1, 2, 3], 'name' => ['a', 'b', 'c']],
            ['id' => [4, 5], 'name' => ['d', 'e']],
        ]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['name' => 'b'], ['name' => 'd']],
            iterator_to_array(
                $file->values(parquet_query()->select('name')->where(parquet_column('id')->in([2, 4]))),
                false,
            ),
        );
        static::assertSame(['name', 'id'], $reader->reads[0][0]);
    }

    public function test_offset_and_limit_count_the_matching_rows_and_the_read_stops_at_the_limit(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [
            ['id' => [1, 2, 3, 4]],
            ['id' => [5, 6, 7, 8]],
            ['id' => [9, 10]],
        ]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);
        $middle = parquet_query()
            ->where(parquet_column('id')->greaterThan(1))
            ->where(parquet_column('id')->lessThan(9));

        static::assertSame(
            [['id' => 4], ['id' => 5], ['id' => 6]],
            iterator_to_array($file->values($middle->offset(2)->limit(3)), false),
        );
        static::assertSame(2, $reader->chunksRead);
    }

    public function test_a_filtered_read_chunks_the_matching_rows_by_the_batch_size(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [
            ['id' => [1, 2, 3, 4, 5]],
            ['id' => [6, 7]],
        ]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['id' => [2, 3]], ['id' => [4, 5]], ['id' => [6, 7]]],
            iterator_to_array($file->columns(parquet_query()->where(parquet_column('id')->greaterThan(1)), 2), false),
        );
    }

    public function test_a_limit_of_zero_beside_a_filter_reads_nothing(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [['id' => [1]]]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [],
            iterator_to_array($file->values(parquet_query()->where(parquet_column('id')->equals(1))->limit(0))),
        );
        static::assertSame([], $reader->reads);
    }

    public function test_a_filter_on_a_struct_leaf_reads_its_root_column(): void
    {
        $reader = new SpyParquetFileReader(
            Schema::with(FlatColumn::int64('id'), NestedColumn::struct('address', [FlatColumn::string('city')])),
            [['id' => [1, 2, 3], 'address' => [['city' => 'x'], null, ['city' => 'y']]]],
        );
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            iterator_to_array(
                $file->values(parquet_query()->select('id')->where(parquet_column('address.city')->isNotNull())),
                false,
            ),
        );
        static::assertSame(['id', 'address'], $reader->reads[0][0]);
    }

    public function test_a_filter_literal_the_column_cannot_hold_is_refused(): void
    {
        $file = new ParquetFile(
            new MemorySourceStream('PAR1'),
            new Options(),
            new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [['id' => [1]]]),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "id" (INT64) cannot hold the string literal \'1\'');

        iterator_to_array($file->values(parquet_query()->where(parquet_column('id')->equals('1'))));
    }

    public function test_page_headers_are_each_column_chunk_pages_labelled_with_its_column(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            Writer::php(),
            Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
        ));

        static::assertSame(
            [['id', 'id'], ['name', 'name']],
            array_map(
                static fn(ColumnPageHeader $header): array => [
                    $header->column->flatPath(),
                    $header->columnChunk->flatPath(),
                ],
                iterator_to_array($file->pageHeaders(), false),
            ),
        );
    }

    public function test_the_page_indexes_are_built_once(): void
    {
        $file = new ParquetFile(
            new MemorySourceStream('PAR1'),
            new Options(),
            new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'))),
        );

        static::assertSame($file->indexes(), $file->indexes());
    }

    public function test_a_filtered_read_reads_each_offset_index_once(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footerOnly = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $footerFile = Reader::php()->readStream($footerOnly);
        $footerFile->reader()->footer();
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $file = Reader::php()->readStream($stream);
        $id = $file->schema()->getFlat('id');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id);
        $pages = $file->indexes()->offsetIndex(0, $id)->pages ?? static::fail('No offset index');
        $read = $stream->bytes;

        static::assertSame(
            range(5, 11),
            array_column(
                iterator_to_array(
                    $file->values(parquet_query()->select('id')->where(parquet_column('id')->greaterThanOrEqual(5))),
                    false,
                ),
                'id',
            ),
        );
        static::assertSame(
            $footerOnly->bytes
            + (int) $chunk->columnIndex()?->length
            + (int) $chunk->offsetIndex()?->length
            + $pages[2]->compressedPageSize
            + $pages[3]->compressedPageSize
            + $pages[4]->compressedPageSize
            + $pages[5]->compressedPageSize,
            $stream->bytes,
        );
        static::assertSame($footerOnly->bytes + (int) $chunk->offsetIndex()?->length, $read);
    }

    public function test_an_unfiltered_window_reads_only_the_pages_it_selects(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $footerOnly = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $footerFile = Reader::php()->readStream($footerOnly);
        $footerFile->reader()->footer();
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $file = Reader::php()->readStream($stream);
        $id = $file->schema()->getFlat('id');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($id);
        $pages = $file->indexes()->offsetIndex(0, $id)->pages ?? static::fail('No offset index');

        static::assertSame(
            [5, 6],
            array_column(
                iterator_to_array($file->values(parquet_query()->select('id')->offset(5)->limit(2)), false),
                'id',
            ),
        );
        static::assertSame(
            $footerOnly->bytes
            + (int) $chunk->offsetIndex()?->length
            + $pages[2]->compressedPageSize
            + $pages[3]->compressedPageSize,
            $stream->bytes,
        );
    }
}
