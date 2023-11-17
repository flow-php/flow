<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\split;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class SplitTest extends TestCase
{
    public function test_split() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => '1-2'],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => '1-2', 'split' => ['1', '2']],
            ],
            $memory->data
        );
    }

    public function test_split_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 1],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 1, 'split' => 1],
            ],
            $memory->data
        );
    }

    public function test_split_with_missing_separator() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => '1'],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => '1', 'split' => ['1']],
            ],
            $memory->data
        );
    }
}
