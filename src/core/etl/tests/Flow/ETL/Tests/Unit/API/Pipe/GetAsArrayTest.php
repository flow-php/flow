<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\get_as_array;
use function iterator_to_array;

final class GetAsArrayTest extends FlowTestCase
{
    public function test_get_as_array_yields_each_batch_as_an_array(): void
    {
        static::assertSame(
            [[['a' => 1]], [['a' => 2]]],
            iterator_to_array(get_as_array()(df()->read(from_array([['a' => 1], ['a' => 2]]))->batchSize(1)), false),
        );
    }
}
