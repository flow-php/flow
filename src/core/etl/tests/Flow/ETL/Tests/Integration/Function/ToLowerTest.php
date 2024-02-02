<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
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

        $this->assertSame(
            [
                ['key' => 'VALUE', 'to_lower' => 'value'],
            ],
            $memory->dump()
        );
    }

    public function test_to_lower_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('to_lower', ref('id')->lower())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'to_lower' => 1],
            ],
            $memory->dump()
        );
    }
}
