<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class SprintfTest extends TestCase
{
    public function test_sprintf() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'test %s'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('key')->sprintf(lit('value')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'test %s', 'sprintf' => 'test value'],
            ],
            $memory->data
        );
    }

    public function test_sprintf_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit('1')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'sprintf' => null],
            ],
            $memory->data
        );
    }

    public function test_sprintf_on_null_value() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('sprintf', ref('id')->sprintf(lit(null)))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'sprintf' => null],
            ],
            $memory->data
        );
    }
}
