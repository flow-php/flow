<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length(): void
    {
        data_frame()
            ->read(from_array([
                ['text' => 'hello'],
                ['text' => 'world🚀'],
                ['text' => 'café'],
                ['text' => 'नमस्ते'],
                ['text' => ''],
                ['text' => null],
                ['text' => 'a'],
                ['text' => str_repeat('x', 100)],
                ['text' => 'é'],
                ['text' => "\x00\x01\x02\xFF"],
            ]))
            ->withEntry('binary_length', optional(ref('text')->binaryLength()))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['text' => 'hello', 'binary_length' => 5],
                ['text' => 'world🚀', 'binary_length' => 9],
                ['text' => 'café', 'binary_length' => 5],
                ['text' => 'नमस्ते', 'binary_length' => 18],
                ['text' => '', 'binary_length' => 0],
                ['text' => null, 'binary_length' => null],
                ['text' => 'a', 'binary_length' => 1],
                ['text' => str_repeat('x', 100), 'binary_length' => 100],
                ['text' => 'é', 'binary_length' => 2],
                ['text' => "\x00\x01\x02\xFF", 'binary_length' => 4],
            ],
            $memory->dump(),
        );
    }
}
