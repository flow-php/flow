<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\array_key_rename;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
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

        $this->assertSame(
            [
                ['id' => 1, 'array' => ['b' => 2, 'c' => 3, 'd' => 1]],
                ['id' => 2, 'array' => null],
            ],
            $memory->data
        );
    }
}
