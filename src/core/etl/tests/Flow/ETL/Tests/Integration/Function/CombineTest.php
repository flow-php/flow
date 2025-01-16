<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{combine, from_array, optional, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class CombineTest extends FlowTestCase
{
    public function test_combine() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'first' => ['a', 'b', 'c'], 'second' => [1, 2, 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('array', optional(combine(ref('first'), ref('second'))))
            ->drop('first', 'second')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => null],
            ],
            $memory->dump()
        );
    }
}
