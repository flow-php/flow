<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class CountTest extends TestCase
{
    public function test_count_on_array() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['array' => [1, 2, 3]],
                    ]
                )
            )
            ->withEntry('count', ref('array')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['array' => [1, 2, 3], 'count' => 3],
            ],
            $memory->dump()
        );
    }

    public function test_count_on_non_countable() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 1],
                    ]
                )
            )
            ->withEntry('count', ref('key')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 1, 'count' => null],
            ],
            $memory->dump()
        );
    }

    public function test_count_on_object() : void
    {
        $iterator = new \ArrayIterator([1, 2, 3]);

        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => $iterator],
                    ]
                )
            )
            ->withEntry('count', ref('key')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertEquals(
            [
                ['key' => $iterator, 'count' => 3],
            ],
            $memory->dump()
        );
    }
}
