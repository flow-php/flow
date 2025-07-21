<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, lit, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class StringMatchAllTest extends FlowTestCase
{
    public function test_string_match_all_email_extraction() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['content' => 'Contact: user@example.com and admin@test.org for support'],
                    ]
                )
            )
            ->withEntry('emails', ref('content')->stringMatchAll(lit('/(\w+)@(\w+\.\w+)/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                [
                    'content' => 'Contact: user@example.com and admin@test.org for support',
                    'emails' => [
                        ['user@example.com', 'user', 'example.com'],
                        ['admin@test.org', 'admin', 'test.org'],
                    ],
                ],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_extracting_multiple_data_points() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['log' => '[2023-12-01] ERROR: Something went wrong [2023-12-01] INFO: All fixed'],
                        ['log' => '[2023-12-02] WARNING: Check this [2023-12-02] DEBUG: Details here'],
                    ]
                )
            )
            ->withEntry('extracted', ref('log')->stringMatchAll(lit('/\[(\d{4}-\d{2}-\d{2})\]\s+(\w+):\s+([^[]+)/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $expected = [
            [
                'log' => '[2023-12-01] ERROR: Something went wrong [2023-12-01] INFO: All fixed',
                'extracted' => [
                    ['[2023-12-01] ERROR: Something went wrong ', '2023-12-01', 'ERROR', 'Something went wrong '],
                    ['[2023-12-01] INFO: All fixed', '2023-12-01', 'INFO', 'All fixed'],
                ],
            ],
            [
                'log' => '[2023-12-02] WARNING: Check this [2023-12-02] DEBUG: Details here',
                'extracted' => [
                    ['[2023-12-02] WARNING: Check this ', '2023-12-02', 'WARNING', 'Check this '],
                    ['[2023-12-02] DEBUG: Details here', '2023-12-02', 'DEBUG', 'Details here'],
                ],
            ],
        ];

        self::assertSame($expected, $memory->dump());
    }

    public function test_string_match_all_multiple_rows() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'price: 19.99 and 5.50'],
                        ['text' => 'no prices here'],
                        ['text' => 'total: 100.00'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\d+\.\d+/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'price: 19.99 and 5.50', 'match_results' => [['19.99'], ['5.50']]],
                ['text' => 'no prices here', 'match_results' => []],
                ['text' => 'total: 100.00', 'match_results' => [['100.00']]],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_no_matches() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\d+/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_results' => []],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_successful() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'test 123 and 456 and 789'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\d+/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'test 123 and 456 and 789', 'match_results' => [['123'], ['456'], ['789']]],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_with_capturing_groups() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['config' => 'width=100 height=200 depth=50'],
                    ]
                )
            )
            ->withEntry('match_results', ref('config')->stringMatchAll(lit('/(\w+)=(\d+)/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        $expected = [
            [
                'config' => 'width=100 height=200 depth=50',
                'match_results' => [
                    ['width=100', 'width', '100'],
                    ['height=200', 'height', '200'],
                    ['depth=50', 'depth', '50'],
                ],
            ],
        ];

        self::assertSame($expected, $memory->dump());
    }

    public function test_string_match_all_with_invalid_regex() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/[/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'match_results' => []],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_with_null_input() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => null],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\d+/')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => null, 'match_results' => []],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_with_scalar_function_pattern() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'test 123 and 456', 'pattern' => '/\d+/'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(ref('pattern')))
            ->drop('pattern')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'test 123 and 456', 'match_results' => [['123'], ['456']]],
            ],
            $memory->dump()
        );
    }

    public function test_string_match_all_with_unicode() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'नमस्ते world स्वागत'],
                    ]
                )
            )
            ->withEntry('match_results', ref('text')->stringMatchAll(lit('/\S+/u')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'नमस्ते world स्वागत', 'match_results' => [['नमस्ते'], ['world'], ['स्वागत']]],
            ],
            $memory->dump()
        );
    }
}
