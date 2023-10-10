<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ToLowerTest extends TestCase
{
    public function test_to_lower() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'VALUE'],
                    ]
                )
            )
            ->withEntry('to_lower', ref('key')->lower())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'VALUE', 'to_lower' => 'value'],
            ],
            $memory->data
        );
    }

    public function test_to_lower_on_non_string_key() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['id' => 1],
                    ]
                )
            )
            ->withEntry('to_lower', ref('id')->lower())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['id' => 1, 'to_lower' => 1],
            ],
            $memory->data
        );
    }
}
