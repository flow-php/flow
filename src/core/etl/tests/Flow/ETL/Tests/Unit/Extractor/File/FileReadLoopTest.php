<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Column\PhpBackend;
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
use function Flow\ETL\DSL\int_schema;
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
            new PhpBackend(),
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
            new PhpBackend(),
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
            new PhpBackend(),
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
            new PhpBackend(),
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
            new PhpBackend(),
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
            ))->read([], new InMemoryFileBatches([]), 10, new PhpBackend(), new ReadWindow())),
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
                new PhpBackend(),
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
                new PhpBackend(),
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
}
