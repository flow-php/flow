<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\cross_join;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class CrossJoinTest extends FlowTestCase
{
    public function test_cross_join_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'x']],
            cross_join(df()->read(from_array([['b' => 'x']])))(df()->read(from_array([['a' => 1], ['a' => 2]])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_cross_join_through_with(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'x']],
            df()
                ->read(from_array([['a' => 1], ['a' => 2]]))
                ->with(cross_join(df()->read(from_array([['b' => 'x']]))))
                ->fetch()
                ->toArray(),
        );
    }
}
