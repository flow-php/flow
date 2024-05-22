<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class StrPadTest extends TestCase
{
    public function test_strpad() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('strpad', ref('key')->strPad(10, '*'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'value', 'strpad' => 'value*****'],
            ],
            $memory->dump()
        );
    }

    public function test_strpad_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('strpad', ref('id')->strPad(10))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'strpad' => '1         '],
            ],
            $memory->dump()
        );
    }
}
