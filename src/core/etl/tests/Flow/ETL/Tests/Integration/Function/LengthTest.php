<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class LengthTest extends FlowTestCase
{
    public function test_length() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello'],
                        ['text' => 'world🚀'],
                        ['text' => 'café'],
                        ['text' => 'नमस्ते'],
                        ['text' => ''],
                        ['text' => null],
                        ['text' => 'a'],
                        ['text' => str_repeat('x', 100)],
                    ]
                )
            )
            ->withEntry('length', ref('text')->length())
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'length' => 5],
                ['text' => 'world🚀', 'length' => 6],
                ['text' => 'café', 'length' => 4],
                ['text' => 'नमस्ते', 'length' => 3],
                ['text' => '', 'length' => 0],
                ['text' => null, 'length' => null],
                ['text' => 'a', 'length' => 1],
                ['text' => str_repeat('x', 100), 'length' => 100],
            ],
            $memory->dump()
        );
    }
}
