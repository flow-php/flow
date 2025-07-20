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

    public function test_index_of_last_with_case_sensitivity() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'Hello World', 'needle' => 'L', 'ignore_case' => false],
                        ['text' => 'Hello World', 'needle' => 'L', 'ignore_case' => true],
                        ['text' => 'HELLO WORLD', 'needle' => 'l', 'ignore_case' => false],
                        ['text' => 'HELLO WORLD', 'needle' => 'l', 'ignore_case' => true],
                    ]
                )
            )
            ->withEntry('last_index', ref('text')->indexOfLast(ref('needle'), ref('ignore_case')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'Hello World', 'needle' => 'L', 'ignore_case' => false, 'last_index' => null],
                ['text' => 'Hello World', 'needle' => 'L', 'ignore_case' => true, 'last_index' => 9],
                ['text' => 'HELLO WORLD', 'needle' => 'l', 'ignore_case' => false, 'last_index' => null],
                ['text' => 'HELLO WORLD', 'needle' => 'l', 'ignore_case' => true, 'last_index' => 9],
            ],
            $memory->dump()
        );
    }

    public function test_index_of_last_with_parsing_use_case() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['filepath' => '/path/to/my/file.txt'],
                        ['filepath' => '/another/path/file.log'],
                        ['filepath' => 'simple.csv'],
                        ['filepath' => '/complex/path.with.dots/file.name.ext'],
                    ]
                )
            )
            ->withEntry('last_slash_pos', ref('filepath')->indexOfLast('/'))
            ->withEntry('last_dot_pos', ref('filepath')->indexOfLast('.'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['filepath' => '/path/to/my/file.txt', 'last_slash_pos' => 11, 'last_dot_pos' => 16],
                ['filepath' => '/another/path/file.log', 'last_slash_pos' => 13, 'last_dot_pos' => 18],
                ['filepath' => 'simple.csv', 'last_slash_pos' => null, 'last_dot_pos' => 6],
                ['filepath' => '/complex/path.with.dots/file.name.ext', 'last_slash_pos' => 23, 'last_dot_pos' => 33],
            ],
            $memory->dump()
        );
    }
}
