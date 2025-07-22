<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class AppendTest extends FlowTestCase
{
    public function test_append_basic_operation() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => 'John'],
                    ]
                )
            )
            ->withEntry('greeting', ref('name')->append(lit(' Doe')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => 'John', 'greeting' => 'John Doe'],
            ],
            $memory->dump()
        );
    }
}
