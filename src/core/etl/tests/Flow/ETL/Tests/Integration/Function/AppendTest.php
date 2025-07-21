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

    public function test_append_building_file_paths() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['directory' => '/home/user', 'filename' => 'document.txt'],
                        ['directory' => '/var/log', 'filename' => 'app.log'],
                    ]
                )
            )
            ->withEntry('full_path', ref('directory')->append(lit('/'))->append(ref('filename')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['directory' => '/home/user', 'filename' => 'document.txt', 'full_path' => '/home/user/document.txt'],
                ['directory' => '/var/log', 'filename' => 'app.log', 'full_path' => '/var/log/app.log'],
            ],
            $memory->dump()
        );
    }

    public function test_append_building_html_content() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['tag_open' => '[div]', 'content' => 'Hello World', 'tag_close' => '[/div]'],
                    ]
                )
            )
            ->withEntry('html', ref('tag_open')->append(ref('content'))->append(ref('tag_close')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                [
                    'tag_open' => '[div]',
                    'content' => 'Hello World',
                    'tag_close' => '[/div]',
                    'html' => '[div]Hello World[/div]',
                ],
            ],
            $memory->dump()
        );
    }

    public function test_append_building_sql_queries() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['base_query' => 'SELECT * FROM users', 'condition' => ' WHERE active = 1'],
                        ['base_query' => 'SELECT name FROM products', 'condition' => ' WHERE price > 100'],
                    ]
                )
            )
            ->withEntry('full_query', ref('base_query')->append(ref('condition')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['base_query' => 'SELECT * FROM users', 'condition' => ' WHERE active = 1', 'full_query' => 'SELECT * FROM users WHERE active = 1'],
                ['base_query' => 'SELECT name FROM products', 'condition' => ' WHERE price > 100', 'full_query' => 'SELECT name FROM products WHERE price > 100'],
            ],
            $memory->dump()
        );
    }

    public function test_append_building_urls() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['base_url' => 'https://api.example.com', 'endpoint' => '/users'],
                        ['base_url' => 'https://api.test.com', 'endpoint' => '/products'],
                    ]
                )
            )
            ->withEntry('full_url', ref('base_url')->append(ref('endpoint')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['base_url' => 'https://api.example.com', 'endpoint' => '/users', 'full_url' => 'https://api.example.com/users'],
                ['base_url' => 'https://api.test.com', 'endpoint' => '/products', 'full_url' => 'https://api.test.com/products'],
            ],
            $memory->dump()
        );
    }

    public function test_append_multiple_operations() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['part1' => 'Hello', 'part2' => ' beautiful', 'part3' => ' world'],
                    ]
                )
            )
            ->withEntry('result', ref('part1')->append(ref('part2'))->append(ref('part3')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['part1' => 'Hello', 'part2' => ' beautiful', 'part3' => ' world', 'result' => 'Hello beautiful world'],
            ],
            $memory->dump()
        );
    }

    public function test_append_with_empty_strings() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['prefix' => '', 'suffix' => 'content'],
                        ['prefix' => 'prefix', 'suffix' => ''],
                        ['prefix' => '', 'suffix' => ''],
                    ]
                )
            )
            ->withEntry('result', ref('prefix')->append(ref('suffix')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['prefix' => '', 'suffix' => 'content', 'result' => 'content'],
                ['prefix' => 'prefix', 'suffix' => '', 'result' => 'prefix'],
                ['prefix' => '', 'suffix' => '', 'result' => ''],
            ],
            $memory->dump()
        );
    }

    public function test_append_with_null_values() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => null],
                        ['name' => 'Jane'],
                    ]
                )
            )
            ->withEntry('full_name', ref('name')->append(lit(' Smith')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => null, 'full_name' => null],
                ['name' => 'Jane', 'full_name' => 'Jane Smith'],
            ],
            $memory->dump()
        );
    }

    public function test_append_with_scalar_function_suffix() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['first' => 'John', 'last' => ' Doe'],
                        ['first' => 'Jane', 'last' => ' Smith'],
                    ]
                )
            )
            ->withEntry('full_name', ref('first')->append(ref('last')))
            ->drop('last')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['first' => 'John', 'full_name' => 'John Doe'],
                ['first' => 'Jane', 'full_name' => 'Jane Smith'],
            ],
            $memory->dump()
        );
    }

    public function test_append_with_unicode_content() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['greeting' => 'नमस्ते'],
                    ]
                )
            )
            ->withEntry('complete_greeting', ref('greeting')->append(lit(' दुनिया')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['greeting' => 'नमस्ते', 'complete_greeting' => 'नमस्ते दुनिया'],
            ],
            $memory->dump()
        );
    }
}
