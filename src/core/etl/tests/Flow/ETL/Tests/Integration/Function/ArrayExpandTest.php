<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_expand, from_array, ref, to_memory};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayExpandTest extends FlowTestCase
{
    public function test_expand_both() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::BOTH))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'expanded' => ['a' => 1]],
                ['id' => 1, 'expanded' => ['b' => 2]],
                ['id' => 1, 'expanded' => ['c' => 3]],
            ],
            $memory->dump()
        );
    }

    public function test_expand_keys() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('expanded', array_expand(ref('array'), ArrayExpand::KEYS))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'expanded' => 'a'],
                ['id' => 1, 'expanded' => 'b'],
                ['id' => 1, 'expanded' => 'c'],
            ],
            $memory->dump()
        );
    }

    public function test_expand_values() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                    ]
                )
            )
            ->withEntry('expanded', array_expand(ref('array')))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'expanded' => 1],
                ['id' => 1, 'expanded' => 2],
                ['id' => 1, 'expanded' => 3],
            ],
            $memory->dump()
        );
    }
}
