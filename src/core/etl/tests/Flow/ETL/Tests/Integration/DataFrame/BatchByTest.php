<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, from_array, lit, ref};
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class BatchByTest extends FlowIntegrationTestCase
{
    public function test_batch_by_column_with_min_size() : void
    {
        $batchCount = 0;
        $batchSizes = [];

        df()
            ->read(from_array([
                ['order_id' => 1, 'item' => 'A'],
                ['order_id' => 2, 'item' => 'B'],
                ['order_id' => 3, 'item' => 'C'],
                ['order_id' => 4, 'item' => 'D'],
                ['order_id' => 5, 'item' => 'E'],
            ]))
            ->batchBy('order_id', minSize: 3)
            ->run(callback: function ($rows) use (&$batchCount, &$batchSizes) : void {
                $batchCount++;
                $batchSizes[] = $rows->count();
            });

        self::assertSame(2, $batchCount);
        self::assertSame([3, 2], $batchSizes);
    }

    public function test_batch_by_column_without_min_size() : void
    {
        $results = [];
        $batchCount = 0;

        df()
            ->read(from_array([
                ['order_id' => 1, 'item' => 'Widget', 'qty' => 2],
                ['order_id' => 1, 'item' => 'Gadget', 'qty' => 1],
                ['order_id' => 2, 'item' => 'Widget', 'qty' => 5],
                ['order_id' => 2, 'item' => 'Gizmo', 'qty' => 3],
                ['order_id' => 3, 'item' => 'Widget', 'qty' => 1],
            ]))
            ->batchBy('order_id')
            ->run(callback: function ($rows) use (&$results, &$batchCount) : void {
                $batchCount++;
                $results = \array_merge($results, $rows->toArray());
            });

        self::assertSame(3, $batchCount);
        self::assertCount(5, $results);
    }

    public function test_batch_by_preserves_referential_integrity() : void
    {
        $batches = [];

        df()
            ->read(from_array([
                ['order_id' => 1, 'line' => 1],
                ['order_id' => 1, 'line' => 2],
                ['order_id' => 1, 'line' => 3],
                ['order_id' => 2, 'line' => 1],
                ['order_id' => 2, 'line' => 2],
            ]))
            ->batchBy('order_id')
            ->run(callback: function ($rows) use (&$batches) : void {
                $orderIds = \array_unique(\array_column($rows->toArray(), 'order_id'));
                $batches[] = $orderIds;
            });

        self::assertSame([1], $batches[0]);
        self::assertSame([2], $batches[1]);
    }

    public function test_batch_by_using_reference_object() : void
    {
        $batchCount = 0;

        df()
            ->read(from_array([
                ['customer_id' => 'A', 'order' => 1],
                ['customer_id' => 'A', 'order' => 2],
                ['customer_id' => 'B', 'order' => 3],
            ]))
            ->batchBy(ref('customer_id'))
            ->run(callback: function ($rows) use (&$batchCount) : void {
                $batchCount++;
            });

        self::assertSame(2, $batchCount);
    }

    public function test_batch_by_with_large_group_exceeding_min_size() : void
    {
        $batchCount = 0;
        $batchSizes = [];

        df()
            ->read(from_array([
                ['order_id' => 1, 'item' => 'A'],
                ['order_id' => 1, 'item' => 'B'],
                ['order_id' => 1, 'item' => 'C'],
                ['order_id' => 1, 'item' => 'D'],
                ['order_id' => 1, 'item' => 'E'],
                ['order_id' => 2, 'item' => 'F'],
            ]))
            ->batchBy('order_id', minSize: 2)
            ->run(callback: function ($rows) use (&$batchCount, &$batchSizes) : void {
                $batchCount++;
                $batchSizes[] = $rows->count();
            });

        self::assertSame(2, $batchCount);
        self::assertSame([5, 1], $batchSizes);
    }

    public function test_batch_by_with_transformations() : void
    {
        $results = [];

        df()
            ->read(from_array([
                ['order_id' => 1, 'amount' => 100],
                ['order_id' => 1, 'amount' => 200],
                ['order_id' => 2, 'amount' => 300],
            ]))
            ->batchBy('order_id')
            ->withEntry('total', ref('amount')->multiply(lit(2)))
            ->run(callback: function ($rows) use (&$results) : void {
                $results = \array_merge($results, $rows->toArray());
            });

        self::assertSame(200, $results[0]['total']);
        self::assertSame(400, $results[1]['total']);
        self::assertSame(600, $results[2]['total']);
    }
}
