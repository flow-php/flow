<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\aggregate_by;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\sum;

final class AggregateByTest extends FlowTestCase
{
    public function test_aggregate_by_through_invoke(): void
    {
        static::assertSame(
            [['country' => 'PL', 'a_sum' => 3.0], ['country' => 'US', 'a_sum' => 3.0]],
            aggregate_by(new GroupBy(refs('country'), new Aggregators(sum(ref('a')))))(df()->read(from_array([
                ['country' => 'PL', 'a' => 1],
                ['country' => 'PL', 'a' => 2],
                ['country' => 'US', 'a' => 3],
            ])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_aggregate_by_through_with(): void
    {
        static::assertSame(
            [['country' => 'PL', 'a_sum' => 3.0], ['country' => 'US', 'a_sum' => 3.0]],
            df()
                ->read(from_array([
                    ['country' => 'PL', 'a' => 1],
                    ['country' => 'PL', 'a' => 2],
                    ['country' => 'US', 'a' => 3],
                ]))
                ->with(aggregate_by(new GroupBy(refs('country'), new Aggregators(sum(ref('a'))))))
                ->fetch()
                ->toArray(),
        );
    }
}
