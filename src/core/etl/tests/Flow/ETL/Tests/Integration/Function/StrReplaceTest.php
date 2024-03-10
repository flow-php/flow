<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class StrReplaceTest extends TestCase
{
    public function test_str_replace() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('str_replace', ref('key')->strReplace('e', 'es'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'str_replace' => 'values'],
            ],
            $memory->dump()
        );
    }

    public function test_str_replace_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('str_replace', ref('id')->strReplace('', ''))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'str_replace' => null],
            ],
            $memory->dump()
        );
    }
}
