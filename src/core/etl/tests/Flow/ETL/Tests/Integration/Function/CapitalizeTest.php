<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class CapitalizeTest extends FlowTestCase
{
    public function test_to_lower() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['key' => 'this is title'],
                    ]
                )
            )
            ->withEntry('capitalized', ref('key')->capitalize())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['key' => 'this is title', 'capitalized' => 'This Is Title'],
            ],
            $memory->dump()
        );
    }
}
