<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Double\SpyParquetFileReader;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_fill;
use function array_key_exists;
use function array_map;
use function count;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_query;
use function intdiv;
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

    public function test_a_bound_filter_is_left_to_the_reader_and_its_columns_are_not_read(): void
    {
        $filter = parquet_column('id')->in([2, 4]);
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')), [
            ['name' => ['b']],
            ['name' => ['d']],
        ]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['name' => 'b'], ['name' => 'd']],
            iterator_to_array($file->values(parquet_query()->select('name')->where($filter)), false),
        );
        static::assertSame(['name'], $reader->reads[0][0]);
        static::assertSame($filter, $reader->reads[0][2]->rowFilter?->filter());
    }

    public function test_a_bound_filter_plans_offset_plus_limit_rows_and_drops_the_offset(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [
            ['id' => [2, 3, 4]],
            ['id' => [5, 6]],
            ['id' => [7]],
        ]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);
        $middle = parquet_query()
            ->where(parquet_column('id')->greaterThan(1))
            ->where(parquet_column('id')->lessThan(9));

        static::assertSame(
            [['id' => 4], ['id' => 5], ['id' => 6]],
            iterator_to_array($file->values($middle->offset(2)->limit(3)), false),
        );
        static::assertSame(5, $reader->reads[0][2]->limit);
        static::assertSame(2, $reader->chunksRead);
    }

    public function test_the_unbound_conjuncts_are_evaluated_on_the_rows_the_reader_returns(): void
    {
        $bound = parquet_column('id')->lessThan(3);
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumnMother::uint64('big')), [[
            'id' => [1, 2],
            'big' => ['5', '18446744073709551600'],
        ]]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['id' => 1]],
            iterator_to_array(
                $file->values(
                    parquet_query()
                        ->select('id')
                        ->where($bound)
                        // a float beyond 2^53 has no exact pre-image on UINT_64
                        ->where(parquet_column('big')->lessThan(1.0e18))
                        ->limit(5),
                ),
                false,
            ),
        );
        static::assertSame(['id', 'big'], $reader->reads[0][0]);
        static::assertSame($bound, $reader->reads[0][2]->rowFilter?->filter());
        static::assertNull($reader->reads[0][2]->limit);
    }

    public function test_a_filtered_read_chunks_the_matching_rows_by_the_batch_size(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')), [
            ['id' => [2, 3, 4, 5]],
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

    public function test_a_bound_filter_on_a_struct_leaf_does_not_read_its_root_column(): void
    {
        $reader = new SpyParquetFileReader(
            Schema::with(FlatColumn::int64('id'), NestedColumn::struct('address', [FlatColumn::string('city')])),
            [['id' => [1, 3]]],
        );
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            iterator_to_array(
                $file->values(parquet_query()->select('id')->where(parquet_column('address.city')->isNotNull())),
                false,
            ),
        );
        static::assertSame(['id'], $reader->reads[0][0]);
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

    public function test_a_bound_filter_with_a_limit_reads_only_the_pages_up_to_the_limit(): void
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
            [6, 7],
            array_column(
                iterator_to_array(
                    $file->values(
                        parquet_query()
                            ->select('id')
                            ->where(parquet_column('id')->greaterThanOrEqual(5))
                            ->offset(1)
                            ->limit(2),
                    ),
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
            + $pages[3]->compressedPageSize,
            $stream->bytes,
        );
    }

    public function test_a_filtered_read_on_a_struct_leaf_reads_each_page_of_the_leaf_once(): void
    {
        $bytes = MemoryParquetFile::written(
            Writer::php(options: (new Options())->set(Option::PAGE_SIZE_BYTES, 16)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                2,
            )->set(Option::COLUMNS_ENCODINGS, ['address.city' => Encodings::PLAIN])),
            Schema::with(NestedColumn::struct('address', [FlatColumn::string('city')])),
            array_map(static fn(int $id): array => ['address' => ['city' => 'c' . intdiv($id, 4)]], range(0, 11)),
        );
        $footerOnly = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $footerFile = Reader::php()->readStream($footerOnly);
        $footerFile->reader()->footer();
        $stream = new ReadCountingSourceStream(new MemorySourceStream($bytes));
        $file = Reader::php()->readStream($stream);
        $city = $file->schema()->getFlat('address.city');
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($city);
        $pages = $file->indexes()->offsetIndex(0, $city)->pages ?? static::fail('No offset index');
        $matchingPages = 0;

        foreach ($pages as $page => $location) {
            $end = array_key_exists($page + 1, $pages) ? $pages[$page + 1]->firstRowIndex : 12;
            $matchingPages += $location->firstRowIndex < 8 && $end > 4 ? $location->compressedPageSize : 0;
        }

        $query = parquet_query()->select('address')->where(parquet_column('address.city')->equals('c1'));

        static::assertSame(array_fill(0, 4, ['address' => [
            'city' => 'c1',
        ]]), iterator_to_array($file->values($query), false));
        static::assertGreaterThan(1, count($pages));
        static::assertSame(
            $footerOnly->bytes
            + (int) $chunk->columnIndex()?->length
            + (int) $chunk->offsetIndex()?->length
            + $matchingPages,
            $stream->bytes,
        );
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
