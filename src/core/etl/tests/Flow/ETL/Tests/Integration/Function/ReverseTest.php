<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ReverseTest extends FlowTestCase
{
    public function test_reverse() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'world🚀'],
                        ['text' => 'café'],
                        ['text' => ''],
                        ['text' => null],
                    ]
                )
            )
            ->withEntry('reversed', ref('text')->reverse())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'reversed' => 'olleh'],
                ['text' => 'world🚀', 'reversed' => '🚀dlrow'],
                ['text' => 'café', 'reversed' => 'éfac'],
                ['text' => '', 'reversed' => ''],
                ['text' => null, 'reversed' => null],
            ],
            $memory->dump()
        );
    }
}
