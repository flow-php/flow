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

final class SanitizeTest extends TestCase
{
    public function test_sanitize_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('sanitize', ref('id')->sanitize(lit('1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'sanitize' => null],
            ],
            $memory->dump()
        );
    }

    public function test_sanitize_with_skip_characters() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('sanitize', ref('key')->sanitize(lit('*'), lit(2)))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'sanitize' => 'va***'],
            ],
            $memory->dump()
        );
    }

    public function test_sanitize_without_skip_characters() : void
    {
        (new Flow())
            ->read(
                from_array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('sanitize', ref('key')->sanitize(lit('*')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'sanitize' => '*****'],
            ],
            $memory->dump()
        );
    }
}
