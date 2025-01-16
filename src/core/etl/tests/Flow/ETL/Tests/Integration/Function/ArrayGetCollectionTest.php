<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayGetCollectionTest extends FlowTestCase
{
    public function test_array_get_collection() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => [
                            ['a' => 1, 'b' => 2, 'c' => 3],
                            ['a' => 1, 'b' => 2, 'c' => 3],
                            ['a' => 1, 'b' => 2, 'c' => 3],
                        ]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('result', ref('array')->arrayGetCollection(['a', 'c']))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'result' => [['a' => 1, 'c' => 3], ['a' => 1, 'c' => 3], ['a' => 1, 'c' => 3]]],
                ['id' => 2, 'result' => null],
            ],
            $memory->dump()
        );
    }
}
