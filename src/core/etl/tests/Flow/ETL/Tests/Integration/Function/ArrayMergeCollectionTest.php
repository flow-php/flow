<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, optional, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayMergeCollectionTest extends FlowTestCase
{
    public function test_array_merge_collection() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => [
                            ['a' => 1],
                            ['b' => 2],
                            ['c' => 3],
                        ]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('result', optional(ref('array')->arrayMergeCollection()))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'result' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'result' => null],
            ],
            $memory->dump()
        );
    }
}
