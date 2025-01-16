<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, split, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class SplitTest extends FlowTestCase
{
    public function test_split() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => '1-2'],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => '1-2', 'split' => ['1', '2']],
            ],
            $memory->dump()
        );
    }

    public function test_split_on_non_string_value() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 1],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 1, 'split' => null],
            ],
            $memory->dump()
        );
    }

    public function test_split_with_missing_separator() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => '1'],
                    ]
                )
            )
            ->withEntry('split', split(ref('key'), '-'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => '1', 'split' => ['1']],
            ],
            $memory->dump()
        );
    }
}
