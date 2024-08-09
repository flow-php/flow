<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ToLowerTest extends TestCase
{
    public function test_to_lower() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'VALUE'],
                    ]
                )
            )
            ->withEntry('to_lower', ref('key')->lower())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'VALUE', 'to_lower' => 'value'],
            ],
            $memory->dump()
        );
    }
}
