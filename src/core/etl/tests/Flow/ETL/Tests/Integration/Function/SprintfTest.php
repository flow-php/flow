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

final class SprintfTest extends TestCase
{
    public function test_sprintf() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'test %s'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('key')->sprintf(lit('value')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'test %s', 'sprintf' => 'test value'],
            ],
            $memory->dump()
        );
    }

    public function test_sprintf_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'sprintf' => null],
            ],
            $memory->dump()
        );
    }

    public function test_sprintf_on_null_value() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit(null)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'sprintf' => null],
            ],
            $memory->dump()
        );
    }
}
