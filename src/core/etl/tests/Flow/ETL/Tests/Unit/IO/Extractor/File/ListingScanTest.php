<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\IO\Extractor\File\ListingScan;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\PathPartitionsRow;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

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
        static::assertSame(
            [['memory://dir/a', 'memory://dir/b'], ['memory://dir/c']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        new SourceFiles(MemoryFiles::with([
                            'memory://dir/a' => 'a',
                            'memory://dir/b' => 'b',
                            'memory://dir/c' => 'c',
                        ]), path('memory://dir/*')),
                        new FileColumns([], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context()->backend(),
                        new Criteria(),
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
        static::assertSame(
            [['memory://dir/c']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        new SourceFiles(MemoryFiles::with([
                            'memory://dir/a' => 'a',
                            'memory://dir/b' => 'b',
                            'memory://dir/c' => 'c',
                        ]), path('memory://dir/*')),
                        new FileColumns([], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context()->backend(),
                        new Criteria(conjuncts: [
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
        static::assertCount(1, iterator_to_array(
            (new ListingScan(
                new SourceFiles(MemoryFiles::with([
                    'memory://dir/a' => 'a',
                    'memory://dir/b' => 'b',
                    'memory://dir/c' => 'c',
                ]), path('memory://dir/*')),
                new FileColumns([], new PartitionTypes(), false),
                new PathPartitionsRow(),
            ))->scan(
                flow_context()->backend(),
                new Criteria(limit: 1),
                schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                1,
            ),
            false,
        ));
    }

    public function test_an_offset_skips_the_listed_files_the_filter_keeps(): void
    {
        static::assertSame(
            [['memory://dir/c'], ['memory://dir/d']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        new SourceFiles(MemoryFiles::with([
                            'memory://dir/a' => 'a',
                            'memory://dir/b' => 'b',
                            'memory://dir/c' => 'c',
                            'memory://dir/d' => 'd',
                        ]), path('memory://dir/*')),
                        new FileColumns([], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context()->backend(),
                        new Criteria(offset: 1, conjuncts: [ref('path')->notEquals(lit('memory://dir/a'))]),
                        schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
                        1,
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_a_batch_past_the_limit_is_trimmed_to_it(): void
    {
        static::assertSame(
            [['memory://dir/a', 'memory://dir/b']],
            array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('path'),
                iterator_to_array(
                    (new ListingScan(
                        new SourceFiles(MemoryFiles::with([
                            'memory://dir/a' => 'a',
                            'memory://dir/b' => 'b',
                            'memory://dir/c' => 'c',
                        ]), path('memory://dir/*')),
                        new FileColumns([], new PartitionTypes(), false),
                        new PathPartitionsRow(),
                    ))->scan(
                        flow_context()->backend(),
                        new Criteria(limit: 2),
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
        $criteria = (new ListingScan(
            new SourceFiles(MemoryFiles::with([
                'memory://dir/a' => 'a',
                'memory://dir/b' => 'b',
            ]), path('memory://dir/*')),
            new FileColumns([], new PartitionTypes(), false),
            new PathPartitionsRow(),
        ))->scan(
            flow_context()->backend(),
            new Criteria(),
            schema(str_schema('path'), map_schema('partitions', type_map(type_string(), type_string()))),
            1,
        );

        static::assertSame(['memory://dir/a'], $criteria->current()->reduceToArray('path'));

        $criteria->send(Signal::STOP);

        static::assertFalse($criteria->valid());
    }
}
