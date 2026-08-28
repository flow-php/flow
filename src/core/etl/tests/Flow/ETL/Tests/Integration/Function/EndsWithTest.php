<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class EndsWithTest extends FlowTestCase
{
    public function test_ends_with(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withEntry('ends_with', ref('key')->endsWith(lit('e')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'ends_with' => true],
            ],
            $memory->dump(),
        );
    }

    public function test_ends_with_on_non_string_key(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
            ]))
            ->withEntry('ends_with', optional(ref('id')->endsWith(lit('1'))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'ends_with' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_ends_with_on_non_string_value(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => '1'],
            ]))
            ->withEntry('ends_with', optional(ref('id')->endsWith(lit(1))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => '1', 'ends_with' => null],
            ],
            $memory->dump(),
        );
    }
}
