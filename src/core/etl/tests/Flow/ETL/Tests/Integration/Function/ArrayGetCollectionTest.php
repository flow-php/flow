<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ArrayGetCollectionTest extends TestCase
{
    public function test_array_get_collection() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1, 'array' => [
                            ['a' => 1, 'b' => 2, 'c' => 3],
                            ['a' => 1, 'b' => 2, 'c' => 3],
                            ['a' => 1, 'b' => 2, 'c' => 3],
                        ]],
                        ['id' => 2],
                    ]
                )
            )
            ->withEntry('result', ref('array')->arrayGetCollection('a', 'c'))
            ->drop('array')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'result' => [['a' => 1, 'c' => 3], ['a' => 1, 'c' => 3], ['a' => 1, 'c' => 3]]],
                ['id' => 2, 'result' => null],
            ],
            $memory->data
        );
    }
}
