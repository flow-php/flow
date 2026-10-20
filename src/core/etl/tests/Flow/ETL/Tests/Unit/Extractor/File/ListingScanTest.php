<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Extractor\File\FileColumns;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Extractor\File\ListingScan;
use Flow\ETL\Extractor\File\PartitionColumns;
use Flow\ETL\Extractor\File\PartitionTypes;
use Flow\ETL\Extractor\File\PathPartitionsRow;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class ListingScanTest extends FlowTestCase
{
    public function test_each_listed_file_is_one_row_in_batches_of_the_batch_size(): void
    {
        $filesystem = MemoryFiles::with(['memory://dir/a' => 'a', 'memory://dir/b' => 'b', 'memory://dir/c' => 'c']);

        static::assertSame(
            [['memory://dir/a', 'memory://dir/b'], ['memory://dir/c']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        $filesystem,
                        path('memory://dir/*'),
                        new FileColumns(new PartitionColumns($filesystem), [], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context(),
                        new FileScan(),
                        schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                        2,
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_a_batch_holds_only_the_files_every_conjunct_keeps_and_an_emptied_batch_is_skipped(): void
    {
        $filesystem = MemoryFiles::with(['memory://dir/a' => 'a', 'memory://dir/b' => 'b', 'memory://dir/c' => 'c']);

        static::assertSame(
            [['memory://dir/c']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        $filesystem,
                        path('memory://dir/*'),
                        new FileColumns(new PartitionColumns($filesystem), [], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context(),
                        new FileScan(conjuncts: [
                            ref('path')->notEquals(lit('memory://dir/a')),
                            ref('path')->notEquals(lit('memory://dir/b')),
                        ]),
                        schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                        2,
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_the_scan_stops_once_the_limit_of_rows_is_handed_out(): void
    {
        $filesystem = MemoryFiles::with(['memory://dir/a' => 'a', 'memory://dir/b' => 'b', 'memory://dir/c' => 'c']);

        static::assertCount(1, iterator_to_array(
            (new ListingScan(
                $filesystem,
                path('memory://dir/*'),
                new FileColumns(new PartitionColumns($filesystem), [], new PartitionTypes(), false),
                new PathPartitionsRow(),
            ))->scan(
                flow_context(),
                new FileScan(limit: 1),
                schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                1,
            ),
            false,
        ));
    }

    public function test_a_batch_past_the_limit_is_trimmed_to_it(): void
    {
        $filesystem = MemoryFiles::with(['memory://dir/a' => 'a', 'memory://dir/b' => 'b', 'memory://dir/c' => 'c']);

        static::assertSame(
            [['memory://dir/a', 'memory://dir/b']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        $filesystem,
                        path('memory://dir/*'),
                        new FileColumns(new PartitionColumns($filesystem), [], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context(),
                        new FileScan(limit: 2),
                        schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                        3,
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_a_stop_signal_ends_the_scan(): void
    {
        $filesystem = MemoryFiles::with(['memory://dir/a' => 'a', 'memory://dir/b' => 'b']);
        $scan = (new ListingScan(
            $filesystem,
            path('memory://dir/*'),
            new FileColumns(new PartitionColumns($filesystem), [], new PartitionTypes(), false),
            new PathPartitionsRow(),
        ))->scan(
            flow_context(),
            new FileScan(),
            schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
            1,
        );

        static::assertSame(['memory://dir/a'], $scan->current()->reduceToArray('path'));

        $scan->send(Signal::STOP);

        static::assertFalse($scan->valid());
    }
}
