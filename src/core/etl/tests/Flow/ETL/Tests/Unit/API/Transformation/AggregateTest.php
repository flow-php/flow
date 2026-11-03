<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\aggregate;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\group_by;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

final class AggregateTest extends FlowTestCase
{
    public function test_aggregate_through_invoke(): void
    {
        static::assertSame(
            [['a_sum' => 6.0]],
            aggregate([sum(ref('a'))])(df()->read(from_array([
                ['country' => 'PL', 'a' => 1],
                ['country' => 'PL', 'a' => 2],
                ['country' => 'US', 'a' => 3],
            ])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_aggregate_through_with(): void
    {
        static::assertSame(
            [['a_sum' => 6.0]],
            df()
                ->read(from_array([
                    ['country' => 'PL', 'a' => 1],
                    ['country' => 'PL', 'a' => 2],
                    ['country' => 'US', 'a' => 3],
                ]))
                ->with(aggregate([sum(ref('a'))]))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_aggregate_after_group_by(): void
    {
        static::assertSame(
            [['country' => 'PL', 'a_sum' => 3.0], ['country' => 'US', 'a_sum' => 3.0]],
            aggregate([sum(ref('a'))])(group_by('country')(df()->read(from_array([
                ['country' => 'PL', 'a' => 1],
                ['country' => 'PL', 'a' => 2],
                ['country' => 'US', 'a' => 3],
            ]))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_aggregate_after_group_by_refuses_an_algorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'aggregate() after group_by() takes no bucketing algorithm. Pass it to group_by() instead.',
        );

        aggregate([sum(ref('a'))], hash_group_by())(group_by('country')(df()->read(from_array([
            ['country' => 'PL', 'a' => 1],
            ['country' => 'PL', 'a' => 2],
            ['country' => 'US', 'a' => 3],
        ]))))->fetch();
    }
}
