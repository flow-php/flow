<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_sort, from_array, optional, ref, to_memory};
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArraySortTest extends FlowTestCase
{
    public function test_array_sort() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => ['a' => 1, 'c' => 3, 'b' => 2]],
                        ['id' => 2],
                    ]
                )
            )

            ->withEntry('array', optional(array_sort(ref('array'), Sort::ksort)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['id' => 1, 'array' => ['a' => 1, 'b' => 2, 'c' => 3]],
                ['id' => 2, 'array' => null],
            ],
            $memory->dump()
        );
    }
}
