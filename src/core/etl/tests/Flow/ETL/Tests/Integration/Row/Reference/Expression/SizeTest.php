<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class SizeTest extends TestCase
{
    public function test_size_on_array() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['array' => [1, 2, 3]],
                    ]
                )
            )
            ->withEntry('row', ref('row')->unpack())
            ->renameAll('row.', '')
            ->drop('row')
            ->withEntry('size', ref('array')->size())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['array' => [1, 2, 3], 'size' => 3],
            ],
            $memory->data
        );
    }

    public function test_size_on_non_string_key() : void
    {
        $this->expectException(RuntimeException::class);

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
            ->withEntry('size', ref('id')->size())
            ->run();
    }

    public function test_size_on_string() : void
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
            ->withEntry('size', ref('key')->size())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'value', 'size' => 5],
            ],
            $memory->data
        );
    }
}
