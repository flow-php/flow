<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StartsWithTest extends FlowTestCase
{
    public function test_starts_with() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('starts_with', ref('key')->startsWith(lit('v')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'starts_with' => true],
            ],
            $memory->dump()
        );
    }

    public function test_starts_with_on_non_string_key() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('starts_with', ref('id')->startsWith(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'starts_with' => false],
            ],
            $memory->dump()
        );
    }

    public function test_starts_with_on_non_string_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('starts_with', ref('id')->startsWith(lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'starts_with' => false],
            ],
            $memory->dump()
        );
    }
}
