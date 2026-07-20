<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Row;
use Flow\ETL\Sort\Merge\KWayMerge;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;

final class KWayMergeTest extends FlowTestCase
{
    public function test_merges_multiple_sorted_runs_ascending(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1)), row(int_entry('id', 4)), row(int_entry('id', 7))));
        $storage->append('b', rows(row(int_entry('id', 2)), row(int_entry('id', 5)), row(int_entry('id', 8))));
        $storage->append('c', rows(row(int_entry('id', 3)), row(int_entry('id', 6)), row(int_entry('id', 9))));

        $merge = new KWayMerge($storage, refs(ref('id')->asc()));

        static::assertSame(
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
            array_map(static fn(Row $r): mixed => $r->valueOf('id'), iterator_to_array($merge->merge(['a', 'b', 'c']))),
        );
    }

    public function test_merges_multiple_sorted_runs_descending(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 7)), row(int_entry('id', 4)), row(int_entry('id', 1))));
        $storage->append('b', rows(row(int_entry('id', 8)), row(int_entry('id', 5)), row(int_entry('id', 2))));

        $merge = new KWayMerge($storage, refs(ref('id')->desc()));

        static::assertSame(
            [8, 7, 5, 4, 2, 1],
            array_map(static fn(Row $r): mixed => $r->valueOf('id'), iterator_to_array($merge->merge(['a', 'b']))),
        );
    }

    public function test_merges_runs_of_uneven_length(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))));
        $storage->append('b', rows(row(int_entry('id', 4))));

        $merge = new KWayMerge($storage, refs(ref('id')->asc()));

        static::assertSame(
            [1, 2, 3, 4],
            array_map(static fn(Row $r): mixed => $r->valueOf('id'), iterator_to_array($merge->merge(['a', 'b']))),
        );
    }

    public function test_merges_non_numeric_values(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(str_entry('id', 'a')), row(str_entry('id', 'c'))));
        $storage->append('b', rows(row(str_entry('id', 'b')), row(str_entry('id', 'd'))));

        $merge = new KWayMerge($storage, refs(ref('id')->asc()));

        static::assertSame(
            ['a', 'b', 'c', 'd'],
            array_map(static fn(Row $r): mixed => $r->valueOf('id'), iterator_to_array($merge->merge(['a', 'b']))),
        );
    }

    public function test_single_run_passthrough(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));

        $merge = new KWayMerge($storage, refs(ref('id')->asc()));

        static::assertSame(
            [1, 2],
            array_map(static fn(Row $r): mixed => $r->valueOf('id'), iterator_to_array($merge->merge(['a']))),
        );
    }

    public function test_empty_bucket_set_yields_nothing(): void
    {
        $merge = new KWayMerge(new MemoryBuckets(), refs(ref('id')->asc()));

        static::assertSame([], iterator_to_array($merge->merge([])));
    }
}
