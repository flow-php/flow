<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class SprintfTest extends FlowTestCase
{
    public function test_sprintf() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'test %s'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('key')->sprintf(lit('value')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'test %s', 'sprintf' => 'test value'],
            ],
            $memory->dump()
        );
    }

    public function test_sprintf_on_non_string_key() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'sprintf' => null],
            ],
            $memory->dump()
        );
    }

    public function test_sprintf_on_null_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit(null)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'sprintf' => null],
            ],
            $memory->dump()
        );
    }
}
