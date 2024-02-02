<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class StartsWithTest extends TestCase
{
    public function test_starts_with() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('starts_with', ref('key')->startsWith(lit('v')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'starts_with' => true],
            ],
            $memory->dump()
        );
    }

    public function test_starts_with_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('starts_with', ref('id')->startsWith(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'starts_with' => false],
            ],
            $memory->dump()
        );
    }

    public function test_starts_with_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('starts_with', ref('id')->startsWith(lit(1)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'starts_with' => false],
            ],
            $memory->dump()
        );
    }
}
