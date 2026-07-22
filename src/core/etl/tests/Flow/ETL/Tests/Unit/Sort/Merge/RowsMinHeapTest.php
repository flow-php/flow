<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Sort\Merge\RowsMinHeap;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function count;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function range;

final class RowsMinHeapTest extends FlowTestCase
{
    public function test_min_heap(): void
    {
        $minHeap = new RowsMinHeap(ref('id')->asc());

        $minHeap->push(row(int_entry('id', 1)), 'cache_id');
        $minHeap->push(row(int_entry('id', 2)), 'cache_id');
        $minHeap->push(row(int_entry('id', 3)), 'cache_id');
        $minHeap->push(row(int_entry('id', 4)), 'cache_id');
        $minHeap->push(row(int_entry('id', 5)), 'cache_id');
        $minHeap->push(row(int_entry('id', 6)), 'cache_id');

        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ],
            array_map(static fn() => $minHeap->extract()->row->toArray(), range(1, count($minHeap))),
        );
    }

    public function test_min_heap_desc(): void
    {
        $minHeap = new RowsMinHeap(ref('id')->desc());

        $minHeap->push(row(int_entry('id', 1)), 'cache_id');
        $minHeap->push(row(int_entry('id', 2)), 'cache_id');
        $minHeap->push(row(int_entry('id', 3)), 'cache_id');
        $minHeap->push(row(int_entry('id', 4)), 'cache_id');
        $minHeap->push(row(int_entry('id', 5)), 'cache_id');
        $minHeap->push(row(int_entry('id', 6)), 'cache_id');

        static::assertEquals(
            [
                ['id' => 6],
                ['id' => 5],
                ['id' => 4],
                ['id' => 3],
                ['id' => 2],
                ['id' => 1],
            ],
            array_map(static fn() => $minHeap->extract()->row->toArray(), range(1, count($minHeap))),
        );
    }

    public function test_min_heap_on_non_numeric_values(): void
    {
        $minHeap = new RowsMinHeap(ref('id')->asc());

        $minHeap->push(row(str_entry('id', 'a')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'b')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'c')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'd')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'e')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'f')), 'cache_id');

        static::assertEquals(
            [
                ['id' => 'a'],
                ['id' => 'b'],
                ['id' => 'c'],
                ['id' => 'd'],
                ['id' => 'e'],
                ['id' => 'f'],
            ],
            array_map(static fn() => $minHeap->extract()->row->toArray(), range(1, count($minHeap))),
        );
    }

    public function test_min_heap_on_non_numeric_values_desc(): void
    {
        $minHeap = new RowsMinHeap(ref('id')->desc());

        $minHeap->push(row(str_entry('id', 'a')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'b')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'c')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'd')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'e')), 'cache_id');
        $minHeap->push(row(str_entry('id', 'f')), 'cache_id');

        static::assertEquals(
            [
                ['id' => 'f'],
                ['id' => 'e'],
                ['id' => 'd'],
                ['id' => 'c'],
                ['id' => 'b'],
                ['id' => 'a'],
            ],
            array_map(static fn() => $minHeap->extract()->row->toArray(), range(1, count($minHeap))),
        );
    }
}
