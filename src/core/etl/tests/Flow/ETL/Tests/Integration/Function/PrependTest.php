<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class PrependTest extends FlowTestCase
{
    public function test_prepend_basic_operation() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => 'Doe'],
                    ]
                )
            )
            ->withEntry('greeting', ref('name')->prepend(lit('Mr. ')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => 'Doe', 'greeting' => 'Mr. Doe'],
            ],
            $memory->dump()
        );
    }
}
