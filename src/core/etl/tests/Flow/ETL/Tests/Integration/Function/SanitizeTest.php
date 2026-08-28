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

final class SanitizeTest extends FlowTestCase
{
    public function test_sanitize_on_non_string_key(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
            ]))
            ->withEntry('sanitize', optional(ref('id')->sanitize(lit('1'))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'sanitize' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_sanitize_with_skip_characters(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withEntry('sanitize', ref('key')->sanitize(lit('*'), lit(2)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'sanitize' => 'va***'],
            ],
            $memory->dump(),
        );
    }

    public function test_sanitize_without_skip_characters(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withEntry('sanitize', ref('key')->sanitize(lit('*')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'sanitize' => '*****'],
            ],
            $memory->dump(),
        );
    }
}
