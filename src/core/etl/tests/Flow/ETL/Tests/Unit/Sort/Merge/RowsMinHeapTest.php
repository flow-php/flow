<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Sort\Merge\RowsMinHeap;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function count;
use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function range;

final class RowsMinHeapTest extends FlowTestCase
{
    public function test_min_heap(): void
    {
        $minHeap = new RowsMinHeap(ref('id')->asc());

        $minHeap->push(array_to_row(['id' => 1], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 2], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 3], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 4], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 5], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 6], schema(int_schema('id'))), 'cache_id');

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

        $minHeap->push(array_to_row(['id' => 1], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 2], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 3], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 4], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 5], schema(int_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 6], schema(int_schema('id'))), 'cache_id');

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

        $minHeap->push(array_to_row(['id' => 'a'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'b'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'c'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'd'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'e'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'f'], schema(str_schema('id'))), 'cache_id');

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

        $minHeap->push(array_to_row(['id' => 'a'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'b'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'c'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'd'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'e'], schema(str_schema('id'))), 'cache_id');
        $minHeap->push(array_to_row(['id' => 'f'], schema(str_schema('id'))), 'cache_id');

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
