<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, not, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ContainsTest extends FlowTestCase
{
    public function test_contains() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('contains', ref('key')->contains(lit('a')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'contains' => true],
            ],
            $memory->dump()
        );
    }

    public function test_contains_on_array() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value', 'tags' => ['A', 'B']],
                    ]
                )
            )
            ->withEntry('contains', ref('tags')->contains(lit('A')))
            ->drop('tags')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'contains' => true],
            ],
            $memory->dump()
        );
    }

    public function test_contains_on_non_string_key() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('contains', ref('id')->contains(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'contains' => false],
            ],
            $memory->dump()
        );
    }

    public function test_contains_on_non_string_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('contains', ref('id')->contains(lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'contains' => false],
            ],
            $memory->dump()
        );
    }

    public function test_not_contains_on_array() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value', 'tags' => ['A', 'B']],
                    ]
                )
            )
            ->withEntry('contains', not(ref('tags')->contains(lit('A'))))
            ->drop('tags')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'contains' => false],
            ],
            $memory->dump()
        );
    }
}
