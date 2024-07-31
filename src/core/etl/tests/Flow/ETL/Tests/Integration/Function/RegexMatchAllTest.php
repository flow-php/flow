<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class RegexMatchAllTest extends TestCase
{
    public function test_regex_all() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('preg_match', ref('key')->regexMatchAll(lit('/a/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'preg_match' => true],
            ],
            $memory->dump()
        );
    }

    public function test_regex_all_on_non_integer_flags() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('preg_match', ref('key')->regexMatchAll(lit('1'), lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'preg_match' => null],
            ],
            $memory->dump()
        );
    }

    public function test_regex_all_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('preg_match', ref('id')->regexMatchAll(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'preg_match' => null],
            ],
            $memory->dump()
        );
    }

    public function test_regex_all_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('preg_match', ref('id')->regexMatchAll(lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'preg_match' => null],
            ],
            $memory->dump()
        );
    }
}
