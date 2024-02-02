<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class SizeTest extends TestCase
{
    public function test_size_on_array() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['array' => [1, 2, 3]],
                    ]
                )
            )
            ->withEntry('size', ref('array')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['array' => [1, 2, 3], 'size' => 3],
            ],
            $memory->dump()
        );
    }

    public function test_size_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('size', ref('id')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'size' => null],
            ],
            $memory->dump()
        );
    }

    public function test_size_on_string() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('size', ref('key')->size())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'size' => 5],
            ],
            $memory->dump()
        );
    }
}
