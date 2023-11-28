<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class CapitalizeTest extends TestCase
{
    public function test_to_lower() : void
    {
        (new Flow())
            ->read(
                From::array(
                    [
                        ['key' => 'this is title'],
                    ]
                )
            )
            ->withEntry('capitalized', ref('key')->capitalize())
            ->write(To::memory($memory = new ArrayMemory()))
            ->run();

        $this->assertSame(
            [
                ['key' => 'this is title', 'capitalized' => 'This Is Title'],
            ],
            $memory->data
        );
    }
}
