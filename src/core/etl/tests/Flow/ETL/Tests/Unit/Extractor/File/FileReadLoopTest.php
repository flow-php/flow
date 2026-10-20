<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Extractor\File\FileReadLoop;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Double\InMemoryFileBatches;
use Flow\ETL\Tests\Double\InMemoryUnskippableFileBatches;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FileReadLoopTest extends FlowTestCase
{
    public function test_every_batch_carries_its_files_constants(): void
    {
        $loop = new FileReadLoop(FileColumnsContext::discovering(), schema(int_schema('id'), str_schema('year')));
        $batches = new InMemoryFileBatches([
            'memory://orders/year=2023/a.csv' => [1, 2],
            'memory://orders/year=2024/b.csv' => [3],
        ]);

        $rows = iterator_to_array($loop->read(
            [
                new SourceFile(path('memory://orders/year=2023/a.csv')),
                new SourceFile(path('memory://orders/year=2024/b.csv')),
            ],
            $batches,
            10,
            flow_context(),
            new ReadWindow(),
        ));

        static::assertSame(
            [[['id' => 1, 'year' => '2023'], ['id' => 2, 'year' => '2023']], [['id' => 3, 'year' => '2024']]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
    }

    public function test_stop_after_the_first_batch_ends_the_read(): void
    {
        $read = (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id'))))->read(
            [new SourceFile(path('memory://a.csv')), new SourceFile(path('memory://b.csv'))],
            new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3], 'memory://b.csv' => [4]]),
            1,
            flow_context(),
            new ReadWindow(),
        );

        static::assertSame([['id' => 1]], $read->current()->toArray());

        $read->send(Signal::STOP);

        static::assertFalse($read->valid());
    }

    public function test_a_limit_inside_a_file_ends_the_read_and_is_pushed_down(): void
    {
        $batches = new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3], 'memory://b.csv' => [4]]);

        $rows = iterator_to_array((new FileReadLoop(
            FileColumnsContext::discovering(names: []),
            schema(int_schema('id')),
        ))->read(
            [new SourceFile(path('memory://a.csv')), new SourceFile(path('memory://b.csv'))],
            $batches,
            10,
            flow_context(),
            new ReadWindow(limit: 2),
        ));

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(['memory://a.csv' => [new ReadWindow(0, 2)]], $batches->windows);
    }

    public function test_a_limit_at_a_file_boundary_opens_no_further_file(): void
    {
        $batches = new InMemoryFileBatches(['memory://a.csv' => [1, 2], 'memory://b.csv' => [3]]);

        $rows = iterator_to_array((new FileReadLoop(
            FileColumnsContext::discovering(names: []),
            schema(int_schema('id')),
        ))->read(
            [new SourceFile(path('memory://a.csv')), new SourceFile(path('memory://b.csv'))],
            $batches,
            1,
            flow_context(),
            new ReadWindow(limit: 2),
        ));

        static::assertCount(2, $rows);
        static::assertSame(['memory://a.csv'], array_keys($batches->windows));
    }

    public function test_the_offset_a_file_leaves_and_the_rows_still_wanted_reach_the_next_file(): void
    {
        $batches = new InMemoryFileBatches([
            'memory://a.csv' => [1, 2],
            'memory://b.csv' => [3, 4],
            'memory://c.csv' => [5, 6, 7],
        ]);

        $rows = iterator_to_array((new FileReadLoop(
            FileColumnsContext::discovering(names: []),
            schema(int_schema('id')),
        ))->read(
            [
                new SourceFile(path('memory://a.csv')),
                new SourceFile(path('memory://b.csv')),
                new SourceFile(path('memory://c.csv')),
            ],
            $batches,
            10,
            flow_context(),
            new ReadWindow(offset: 3, limit: 2),
        ));

        static::assertSame(
            [[['id' => 4]], [['id' => 5]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(
            [
                'memory://a.csv' => [new ReadWindow(3, 2)],
                'memory://b.csv' => [new ReadWindow(1, 2)],
                'memory://c.csv' => [new ReadWindow(0, 1)],
            ],
            $batches->windows,
        );
    }

    public function test_no_sources_read_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array((new FileReadLoop(
                FileColumnsContext::discovering(names: []),
                schema(int_schema('id')),
            ))->read([], new InMemoryFileBatches([]), 10, flow_context(), new ReadWindow())),
        );
    }

    public function test_the_loop_slices_the_offset_a_format_cannot_skip_across_files(): void
    {
        $batches = new InMemoryUnskippableFileBatches([
            'memory://a.csv' => [1, 2],
            'memory://b.csv' => [3, 4],
            'memory://c.csv' => [5, 6, 7],
        ]);

        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id'))))->read(
                [
                    new SourceFile(path('memory://a.csv')),
                    new SourceFile(path('memory://b.csv')),
                    new SourceFile(path('memory://c.csv')),
                ],
                $batches,
                10,
                flow_context(),
                new ReadWindow(offset: 3, limit: 2),
            ),
            false,
        );

        static::assertSame(
            [[['id' => 4]], [['id' => 5]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(
            [
                'memory://a.csv' => [new ReadWindow(0, 5)],
                'memory://b.csv' => [new ReadWindow(0, 3)],
                'memory://c.csv' => [new ReadWindow(0, 1)],
            ],
            $batches->windows,
        );
    }

    public function test_the_loop_skips_the_offset_and_trims_to_the_limit_for_a_format_that_cannot_skip(): void
    {
        $batches = new InMemoryUnskippableFileBatches(['memory://a.csv' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);

        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id'))))->read(
                [new SourceFile(path('memory://a.csv'))],
                $batches,
                10,
                flow_context(),
                new ReadWindow(offset: 3, limit: 2),
            ),
            false,
        );

        static::assertSame(
            [[['id' => 4], ['id' => 5]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(['memory://a.csv' => [new ReadWindow(0, 5)]], $batches->windows);
    }

    public function test_a_filter_drops_the_rows_it_does_not_match_and_skips_a_batch_it_empties(): void
    {
        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id')), [ref(
                'id',
            )->greaterThan(lit(
                2,
            ))]))->read([new SourceFile(path('memory://a.csv'))], new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3, 4, 5]]), 2, flow_context(), new ReadWindow()),
            false,
        );

        static::assertSame(
            [[['id' => 3], ['id' => 4]], [['id' => 5]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
    }

    public function test_a_filter_mixing_a_body_and_a_partition_column_sees_the_partition_values(): void
    {
        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(), schema(int_schema('id'), str_schema('year')), [
                ref('year')->equals(lit('2024')),
                ref('id')->greaterThan(lit(3)),
            ]))->read([
                new SourceFile(path('memory://orders/year=2023/a.csv')),
                new SourceFile(path('memory://orders/year=2024/b.csv')),
            ], new InMemoryFileBatches([
                'memory://orders/year=2023/a.csv' => [4, 5],
                'memory://orders/year=2024/b.csv' => [3, 4],
            ]), 10, flow_context(), new ReadWindow()),
            false,
        );

        static::assertSame(
            [[['id' => 4, 'year' => '2024']]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
    }

    public function test_the_offset_counts_raw_rows_and_the_limit_counts_filtered_ones(): void
    {
        $batches = new InMemoryFileBatches(['memory://a.csv' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);

        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id')), [ref(
                'id',
            )->greaterThan(lit(
                5,
            ))]))->read([new SourceFile(path('memory://a.csv'))], $batches, 10, flow_context(), new ReadWindow(offset: 6, limit: 2)),
            false,
        );

        static::assertSame(
            [[['id' => 7], ['id' => 8]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(['memory://a.csv' => [new ReadWindow(6)]], $batches->windows);
    }

    public function test_the_loop_drops_the_raw_offset_before_the_filter_for_a_format_that_cannot_skip(): void
    {
        $batches = new InMemoryUnskippableFileBatches(['memory://a.csv' => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);

        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id')), [ref(
                'id',
            )->greaterThan(lit(
                5,
            ))]))->read([new SourceFile(path('memory://a.csv'))], $batches, 10, flow_context(), new ReadWindow(offset: 6, limit: 2)),
            false,
        );

        static::assertSame(
            [[['id' => 7], ['id' => 8]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(['memory://a.csv' => [new ReadWindow()]], $batches->windows);
    }

    public function test_a_limit_beside_a_filter_counts_matching_rows_across_files_and_opens_no_further_file(): void
    {
        $batches = new InMemoryFileBatches([
            'memory://a.csv' => [1, 2],
            'memory://b.csv' => [3, 4],
            'memory://c.csv' => [5],
        ]);

        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id')), [ref(
                'id',
            )->greaterThan(lit(
                1,
            ))]))->read([
                new SourceFile(path('memory://a.csv')),
                new SourceFile(path('memory://b.csv')),
                new SourceFile(path('memory://c.csv')),
            ], $batches, 10, flow_context(), new ReadWindow(limit: 2)),
            false,
        );

        static::assertSame(
            [[['id' => 2]], [['id' => 3]]],
            array_map(static fn(Rows $batch): array => $batch->toArray(), $rows),
        );
        static::assertEquals(
            ['memory://a.csv' => [new ReadWindow()], 'memory://b.csv' => [new ReadWindow()]],
            $batches->windows,
        );
    }

    public function test_each_conjunct_of_the_filter_sees_only_the_rows_the_ones_before_it_kept(): void
    {
        $rows = iterator_to_array(
            (new FileReadLoop(FileColumnsContext::discovering(names: []), schema(int_schema('id')), [
                ref('id')->notEquals(lit(0)),
                lit(10)->divide(ref('id'))->greaterThan(lit(1)),
            ]))->read([new SourceFile(path('memory://a.csv'))], new InMemoryFileBatches(['memory://a.csv' => [0, 2, 10]]), 10, flow_context(), new ReadWindow()),
            false,
        );

        static::assertSame([[['id' => 2]]], array_map(static fn(Rows $batch): array => $batch->toArray(), $rows));
    }
}
