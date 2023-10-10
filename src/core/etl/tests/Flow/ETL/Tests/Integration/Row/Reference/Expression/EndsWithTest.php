<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class EndsWithTest extends TestCase
{
    public function test_ends_with() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('ends_with', ref('key')->endsWith(lit('e')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'ends_with' => true],
            ],
            $memory->data
        );
    }

    public function test_ends_with_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('ends_with', ref('id')->endsWith(lit('1')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'ends_with' => false],
            ],
            $memory->data
        );
    }

    public function test_ends_with_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('ends_with', ref('id')->endsWith(lit(1)))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'ends_with' => false],
            ],
            $memory->data
        );
    }
}
