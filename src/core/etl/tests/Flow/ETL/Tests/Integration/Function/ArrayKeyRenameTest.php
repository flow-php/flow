<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{array_key_rename, from_array, optional, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayKeyRenameTest extends TestCase
{
    public function test_array_key_rename() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('array', optional(array_key_rename(ref('array'), 'a', 'd')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['b' => 2, 'c' => 3, 'd' => 1]],
                ['id' => 2, 'array' => null],
            ],
            $memory->dump()
        );
    }
}
