<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Kernels\HashJoin\JoinKeys;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\Distribution;
use Flow\ETL\Tests\Double\PassThroughProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\ref;

final class DistributionTest extends FlowTestCase
{
    public function test_clustered_keeps_its_keys_partial_and_final(): void
    {
        $partial = new PassThroughProcessor();
        $final = new CountingProcessor();

        static::assertSame(
            [['country', 'day'], null, [], $partial, $final],
            self::fields(Distribution::clustered(['country', 'day'], $partial, $final)),
        );
    }

    public function test_clustered_without_steps_is_finished_by_the_distributing_processor(): void
    {
        static::assertSame([['country'], null, [], null, null], self::fields(Distribution::clustered(['country'])));
    }

    public function test_co_partitioned_pairs_the_left_and_right_keys(): void
    {
        static::assertSame(
            [['id', 'day'], ['customer_id', 'date'], [], null, null],
            self::fields(Distribution::coPartitioned(new JoinKeys(['id', 'day'], ['customer_id', 'date']))),
        );
    }

    public function test_ordered_keeps_its_order(): void
    {
        $order = [ref('id')->desc(), ref('name')->asc()];

        static::assertSame([[], null, $order, null, null], self::fields(Distribution::ordered($order)));
    }

    public function test_single_keeps_its_partial_and_final(): void
    {
        $partial = new PassThroughProcessor();
        $final = new CountingProcessor();

        static::assertSame([[], null, [], $partial, $final], self::fields(Distribution::single($partial, $final)));
    }

    /**
     * @return list<mixed>
     */
    public static function fields(Distribution $distribution): array
    {
        return [
            $distribution->keys,
            $distribution->rightKeys,
            $distribution->order,
            $distribution->partial,
            $distribution->final,
        ];
    }
}
