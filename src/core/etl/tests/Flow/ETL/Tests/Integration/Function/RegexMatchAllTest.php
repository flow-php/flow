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

final class RegexMatchAllTest extends FlowTestCase
{
    public function test_regex_all(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withEntry('preg_match', ref('key')->regexMatchAll(lit('/a/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'preg_match' => true],
            ],
            $memory->dump(),
        );
    }

    public function test_regex_all_on_non_integer_flags(): void
    {
        data_frame()
            ->read(from_array([
                ['key' => 'value'],
            ]))
            ->withEntry('preg_match', optional(ref('key')->regexMatchAll(lit('1'), lit('1'))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['key' => 'value', 'preg_match' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_regex_all_on_non_string_key(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1],
            ]))
            ->withEntry('preg_match', optional(ref('id')->regexMatchAll(lit('1'))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'preg_match' => null],
            ],
            $memory->dump(),
        );
    }

    public function test_regex_all_on_non_string_value(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => '1'],
            ]))
            ->withEntry('preg_match', optional(ref('id')->regexMatchAll(lit(1))))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => '1', 'preg_match' => null],
            ],
            $memory->dump(),
        );
    }
}
