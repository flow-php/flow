<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\BucketAggregation;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;

final class BucketAggregationTest extends FlowTestCase
{
    public function test_folds_groups_across_multiple_batches(): void
    {
        $context = flow_context();

        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(sum(ref('v')));

        $batches = (static function () {
            yield rows(row(str_entry('k', 'a'), int_entry('v', 1)), row(str_entry('k', 'b'), int_entry('v', 10)));
            yield rows(row(str_entry('k', 'a'), int_entry('v', 2)), row(str_entry('k', 'b'), int_entry('v', 20)));
        })();

        $sums = [];

        foreach ((new BucketAggregation())->aggregate($batches, $context, $groupBy) as $resultRows) {
            foreach ($resultRows as $resultRow) {
                $key = $resultRow->valueOf(ref('k'));
                assert(is_string($key));

                $sums[$key] = $resultRow->valueOf(ref('v_sum'));
            }
        }

        static::assertSame(['a' => 3, 'b' => 30], $sums);
    }

    public function test_first_preserves_input_order_across_batches(): void
    {
        $context = flow_context();

        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(first(ref('v')));

        $batches = (static function () {
            yield rows(row(str_entry('k', 'a'), int_entry('v', 100)));
            yield rows(row(str_entry('k', 'a'), int_entry('v', 200)));
        })();

        $firsts = [];

        foreach ((new BucketAggregation())->aggregate($batches, $context, $groupBy) as $resultRows) {
            foreach ($resultRows as $resultRow) {
                $key = $resultRow->valueOf(ref('k'));
                assert(is_string($key));

                $firsts[$key] = $resultRow->valueOf(ref('v_first'));
            }
        }

        static::assertSame(['a' => 100], $firsts);
    }

    public function test_last_keeps_the_latest_across_batches(): void
    {
        $context = flow_context();

        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(last(ref('v')));

        $batches = (static function () {
            yield rows(row(str_entry('k', 'a'), int_entry('v', 100)));
            yield rows(row(str_entry('k', 'a'), int_entry('v', 200)));
        })();

        $lasts = [];

        foreach ((new BucketAggregation())->aggregate($batches, $context, $groupBy) as $resultRows) {
            foreach ($resultRows as $resultRow) {
                $key = $resultRow->valueOf(ref('k'));
                assert(is_string($key));

                $lasts[$key] = $resultRow->valueOf(ref('v_last'));
            }
        }

        static::assertSame(['a' => 200], $lasts);
    }
}
