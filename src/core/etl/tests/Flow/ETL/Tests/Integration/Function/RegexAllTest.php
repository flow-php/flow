<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class RegexAllTest extends TestCase
{
    public function test_regex_all() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => '124.23 EUR 12 USD 45 PLN'],
                    ]
                )
            )
            ->withEntry('result', ref('key')->regexAll(lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => '124.23 EUR 12 USD 45 PLN', 'result' => [['124.23 EUR', '12 USD', '45 PLN'], ['124.23', '12', '45'], ['EUR', 'USD', 'PLN']]],
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
            ->withEntry('regex', ref('id')->regexAll(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'regex' => null],
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
            ->withEntry('regex', ref('id')->regexAll(lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'regex' => null],
            ],
            $memory->dump()
        );
    }
}
