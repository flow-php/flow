<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayUnpackTest extends TestCase
{
    public function test_array_unpack() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                        ['id' => 2, 'array' => []],
                    ]
                )
            )
            ->withEntry('array', ref('array')->unpack())
            ->renameAll('array.', '')
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'a' => 1, 'b' => 2, 'c' => 3],
                ['id' => 2],
            ],
            $memory->dump()
        );
    }
}
