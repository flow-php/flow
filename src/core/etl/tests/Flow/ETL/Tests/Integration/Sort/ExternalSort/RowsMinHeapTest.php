<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use function Flow\ETL\DSL\{int_entry, ref, row, str_entry};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Sort\{ExternalSort\BucketRow, ExternalSort\RowsMinHeap};

final class RowsMinHeapTest extends TestCase
{
    public function test_min_heap() : void
    {
        $minHeap = new RowsMinHeap(ref('id')->asc());

        $minHeap->insert(new BucketRow(row(int_entry('id', 1)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 2)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 3)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 4)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 5)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 6)), 'cache_id'));

        self::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
            ],
            \array_map(
                fn () => $minHeap->extract()->row->toArray(),
                \range(1, \count($minHeap))
            )
        );
    }

    public function test_min_heap_desc() : void
    {
        $minHeap = new RowsMinHeap(ref('id')->desc());

        $minHeap->insert(new BucketRow(row(int_entry('id', 1)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 2)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 3)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 4)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 5)), 'cache_id'));
        $minHeap->insert(new BucketRow(row(int_entry('id', 6)), 'cache_id'));

        self::assertEquals(
            [
                ['id' => 6],
                ['id' => 5],
                ['id' => 4],
                ['id' => 3],
                ['id' => 2],
                ['id' => 1],
            ],
            \array_map(
                fn () => $minHeap->extract()->row->toArray(),
                \range(1, \count($minHeap))
            )
        );
    }

    public function test_min_heap_on_non_numeric_values() : void
    {
        $minHeap = new RowsMinHeap(ref('id')->asc());

        $minHeap->insert(new BucketRow(row(str_entry('id', 'a')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'b')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'c')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'd')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'e')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'f')), 'cache_id'));

        self::assertEquals(
            [
                ['id' => 'a'],
                ['id' => 'b'],
                ['id' => 'c'],
                ['id' => 'd'],
                ['id' => 'e'],
                ['id' => 'f'],
            ],
            \array_map(
                fn () => $minHeap->extract()->row->toArray(),
                \range(1, \count($minHeap))
            )
        );
    }

    public function test_min_heap_on_non_numeric_values_desc() : void
    {
        $minHeap = new RowsMinHeap(ref('id')->desc());

        $minHeap->insert(new BucketRow(row(str_entry('id', 'a')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'b')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'c')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'd')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'e')), 'cache_id'));
        $minHeap->insert(new BucketRow(row(str_entry('id', 'f')), 'cache_id'));

        self::assertEquals(
            [
                ['id' => 'f'],
                ['id' => 'e'],
                ['id' => 'd'],
                ['id' => 'c'],
                ['id' => 'b'],
                ['id' => 'a'],
            ],
            \array_map(
                fn () => $minHeap->extract()->row->toArray(),
                \range(1, \count($minHeap))
            )
        );
    }
}
