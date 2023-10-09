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

final class StartsWithTest extends TestCase
{
    public function test_starts_with() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'value'],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('starts_with', ref('key')->startsWith(lit('v')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'starts_with' => true],
            ],
            $memory->data
        );
    }

    public function test_starts_with_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('starts_with', ref('id')->startsWith(lit('1')))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'starts_with' => false],
            ],
            $memory->data
        );
    }

    public function test_starts_with_on_non_string_value() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => '1'],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('starts_with', ref('id')->startsWith(lit(1)))
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => '1', 'starts_with' => false],
            ],
            $memory->data
        );
    }
}
