<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\repartition;
use function iterator_to_array;

final class RepartitionTest extends FlowTestCase
{
    public function test_repartition_through_invoke(): void
    {
        static::assertSame(
            [[['a' => 1], ['a' => 1]], [['a' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    repartition('a')(df()->read(from_array([['a' => 1], ['a' => 2], ['a' => 1]]))->batchSize(1))->get(),
                    false,
                ),
            ),
        );
    }

    public function test_repartition_through_with(): void
    {
        static::assertSame(
            [[['a' => 1], ['a' => 1]], [['a' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    df()
                        ->read(from_array([['a' => 1], ['a' => 2], ['a' => 1]]))
                        ->batchSize(1)
                        ->with(repartition('a'))
                        ->get(),
                    false,
                ),
            ),
        );
    }
}
