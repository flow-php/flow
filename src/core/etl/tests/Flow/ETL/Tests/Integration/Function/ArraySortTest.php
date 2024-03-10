<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{array_sort, from_array, optional, ref, to_memory};
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArraySortTest extends TestCase
{
    public function test_array_sort() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'c' => 3, 'b' => 2]],
                        ['id' => 2],
                    ]
                )
            )

            ->withEntry('array', optional(array_sort(ref('array'), 'ksort')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => null],
            ],
            $memory->dump()
        );
    }
}
