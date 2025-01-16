<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, optional, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayReverseTest extends FlowTestCase
{
    public function test_array_reverse() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('array', optional(ref('array'))->arrayReverse())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['c' => 3, 'b' => 2, 'a' => 1]],
                ['id' => 2, 'array' => null],
            ],
            $memory->dump()
        );
    }
}
