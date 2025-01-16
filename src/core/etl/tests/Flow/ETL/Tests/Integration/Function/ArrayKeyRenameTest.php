<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{array_key_rename, from_array, optional, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayKeyRenameTest extends FlowTestCase
{
    public function test_array_key_rename() : void
    {
        (data_frame())
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
