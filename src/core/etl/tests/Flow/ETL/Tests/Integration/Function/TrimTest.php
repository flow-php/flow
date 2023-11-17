<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class TrimTest extends TestCase
{
    public function test_trim_both() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => ' value ', 'trim' => 'value'],
            ],
            $memory->data
        );
    }

    public function test_trim_custom_characters() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => '-value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(characters: '-'))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => '-value ', 'trim' => 'value '],
            ],
            $memory->data
        );
    }

    public function test_trim_left() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(Type::LEFT))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => ' value ', 'trim' => 'value '],
            ],
            $memory->data
        );
    }

    public function test_trim_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('trim', ref('id')->trim())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'trim' => null],
            ],
            $memory->data
        );
    }

    public function test_trim_right() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => ' value '],
                    ]
                )
            )
            ->withEntry('trim', ref('key')->trim(Type::RIGHT))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => ' value ', 'trim' => ' value'],
            ],
            $memory->data
        );
    }
}
