<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class RegexReplaceTest extends FlowTestCase
{
    public function test_regex_replace() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('key')->regexReplace(lit('/e/'), lit('es')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'preg_replace' => 'values'],
            ],
            $memory->dump()
        );
    }

    public function test_regex_replace_on_non_string_key() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('id')->regexReplace(lit('1'), lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'preg_replace' => null],
            ],
            $memory->dump()
        );
    }

    public function test_regex_replace_on_non_string_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('preg_replace', ref('id')->regexReplace(lit(1), lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => '1', 'preg_replace' => null],
            ],
            $memory->dump()
        );
    }
}
