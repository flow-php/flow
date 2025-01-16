<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class SizeTest extends FlowTestCase
{
    public function test_size_on_array() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['array' => [1, 2, 3]],
                    ]
                )
            )
            ->withEntry('size', ref('array')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['array' => [1, 2, 3], 'size' => 3],
            ],
            $memory->dump()
        );
    }

    public function test_size_on_non_string_key() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('size', ref('id')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'size' => null],
            ],
            $memory->dump()
        );
    }

    public function test_size_on_string() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('size', ref('key')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'size' => 5],
            ],
            $memory->dump()
        );
    }
}
