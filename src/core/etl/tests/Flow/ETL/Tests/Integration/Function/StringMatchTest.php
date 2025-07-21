<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchTest extends FlowTestCase
{
    public function test_string_match_extracting_data() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['log' => '[2023-12-01] ERROR: Something went wrong'],
                        ['log' => '[2023-12-02] INFO: All is well'],
                        ['log' => '[2023-12-03] WARNING: Check this'],
                    ]
                )
            )
            ->withEntry('extracted', ref('log')->stringMatch(lit('/\[(\d{4}-\d{2}-\d{2})\]\s+(\w+):\s+(.+)/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $expected = [
            [
                'log' => '[2023-12-01] ERROR: Something went wrong',
                'extracted' => ['[2023-12-01] ERROR: Something went wrong', '2023-12-01', 'ERROR', 'Something went wrong'],
            ],
            [
                'log' => '[2023-12-02] INFO: All is well',
                'extracted' => ['[2023-12-02] INFO: All is well', '2023-12-02', 'INFO', 'All is well'],
            ],
            [
                'log' => '[2023-12-03] WARNING: Check this',
                'extracted' => ['[2023-12-03] WARNING: Check this', '2023-12-03', 'WARNING', 'Check this'],
            ],
        ];

        self::assertSame($expected, $memory->dump());
    }

    public function test_string_match_multiple_rows() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                        ['text' => 'foo bar'],
                        ['text' => 'hello universe'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/hello/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_result' => ['hello']],
                ['text' => 'foo bar', 'match_result' => null],
                ['text' => 'hello universe', 'match_result' => ['hello']],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_no_match() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/foo/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_result' => null],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_successful() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/hello/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_result' => ['hello']],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_with_capturing_groups() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['email' => 'user@example.com'],
                    ]
                )
            )
            ->withEntry('match_result', ref('email')->stringMatch(lit('/(\w+)@(\w+\.\w+)/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['email' => 'user@example.com', 'match_result' => ['user@example.com', 'user', 'example.com']],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_with_invalid_regex() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/[/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_result' => null],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_with_null_input() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => null],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/hello/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => null, 'match_result' => null],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_with_scalar_function_pattern() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'pattern' => '/world/'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(ref('pattern')))
            ->drop('pattern')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_result' => ['world']],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_with_unicode() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'नमस्ते world'],
                    ]
                )
            )
            ->withEntry('match_result', ref('text')->stringMatch(lit('/नमस्ते/u')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'नमस्ते world', 'match_result' => ['नमस्ते']],
            ],
            $memory->dump()
        );
    }
}
