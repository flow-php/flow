<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ToUpperTest extends TestCase
{
    public function test_to_upper() : void
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
            ->withEntry('to_upper', ref('key')->upper())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'to_upper' => 'VALUE'],
            ],
            $memory->data
        );
    }

    public function test_to_upper_on_non_string_key() : void
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
            ->withEntry('to_upper', ref('id')->upper())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'to_upper' => 1],
            ],
            $memory->data
        );
    }
}
