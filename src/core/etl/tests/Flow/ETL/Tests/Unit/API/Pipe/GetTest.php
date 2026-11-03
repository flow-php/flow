<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\get;
use function iterator_to_array;

final class GetTest extends FlowTestCase
{
    public function test_get_yields_each_batch(): void
    {
        static::assertSame(
            [[['a' => 1]], [['a' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(get()(df()->read(from_array([['a' => 1], ['a' => 2]]))->batchSize(1)), false),
            ),
        );
    }
}
