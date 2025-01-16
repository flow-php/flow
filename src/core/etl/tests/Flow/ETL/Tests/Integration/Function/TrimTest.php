<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class TrimTest extends FlowTestCase
{
    public function test_trim_both() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => ' value ', 'trim' => 'value'],
            ],
            $memory->dump()
        );
    }

    public function test_trim_custom_characters() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => '-value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(characters: '-'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => '-value ', 'trim' => 'value '],
            ],
            $memory->dump()
        );
    }

    public function test_trim_left() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(Type::LEFT))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => ' value ', 'trim' => 'value '],
            ],
            $memory->dump()
        );
    }

    public function test_trim_right() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(Type::RIGHT))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => ' value ', 'trim' => ' value'],
            ],
            $memory->dump()
        );
    }
}
