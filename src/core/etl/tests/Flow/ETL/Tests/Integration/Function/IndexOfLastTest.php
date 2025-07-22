<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'needle' => 'l'],
                        ['text' => 'hello world', 'needle' => 'o'],
                        ['text' => 'hello world', 'needle' => 'x'],
                        ['text' => 'hello world', 'needle' => ''],
                        ['text' => '', 'needle' => 'l'],
                        ['text' => null, 'needle' => 'l'],
                        ['text' => 'hello', 'needle' => null],
                        ['text' => 'abababa', 'needle' => 'aba'],
                        ['text' => 'नमस्ते', 'needle' => 'स्ते'],
                    ]
                )
            )
            ->withEntry('last_index', ref('text')->indexOfLast(ref('needle')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'needle' => 'l', 'last_index' => 9],
                ['text' => 'hello world', 'needle' => 'o', 'last_index' => 7],
                ['text' => 'hello world', 'needle' => 'x', 'last_index' => null],
                ['text' => 'hello world', 'needle' => '', 'last_index' => null],
                ['text' => '', 'needle' => 'l', 'last_index' => null],
                ['text' => null, 'needle' => 'l', 'last_index' => false],
                ['text' => 'hello', 'needle' => null, 'last_index' => false],
                ['text' => 'abababa', 'needle' => 'aba', 'last_index' => 4],
                ['text' => 'नमस्ते', 'needle' => 'स्ते', 'last_index' => 2],
            ],
            $memory->dump()
        );
    }
}
