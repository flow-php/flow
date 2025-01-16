<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_merge, from_array, optional, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayMergeTest extends FlowTestCase
{
    public function test_array_merge() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'first' => ['a' => 1, 'b' => 2], 'second' => ['c' => 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('array', optional(array_merge(ref('first'), ref('second'))))
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
