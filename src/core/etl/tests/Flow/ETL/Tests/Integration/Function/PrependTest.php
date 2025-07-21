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

    public function test_prepend_building_commands() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['prefix' => 'sudo ', 'command' => 'systemctl restart nginx'],
                        ['prefix' => 'docker ', 'command' => 'ps -a'],
                    ]
                )
            )
            ->withEntry('full_command', ref('command')->prepend(ref('prefix')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['prefix' => 'sudo ', 'command' => 'systemctl restart nginx', 'full_command' => 'sudo systemctl restart nginx'],
                ['prefix' => 'docker ', 'command' => 'ps -a', 'full_command' => 'docker ps -a'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_building_email_subjects() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['priority' => '[URGENT] ', 'subject' => 'Server maintenance required'],
                        ['priority' => '[INFO] ', 'subject' => 'Weekly newsletter'],
                    ]
                )
            )
            ->withEntry('email_subject', ref('subject')->prepend(ref('priority')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['priority' => '[URGENT] ', 'subject' => 'Server maintenance required', 'email_subject' => '[URGENT] Server maintenance required'],
                ['priority' => '[INFO] ', 'subject' => 'Weekly newsletter', 'email_subject' => '[INFO] Weekly newsletter'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_building_file_paths() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['directory' => '/var/log/', 'filename' => 'app.log'],
                        ['directory' => '/home/user/', 'filename' => 'document.txt'],
                    ]
                )
            )
            ->withEntry('full_path', ref('filename')->prepend(ref('directory')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['directory' => '/var/log/', 'filename' => 'app.log', 'full_path' => '/var/log/app.log'],
                ['directory' => '/home/user/', 'filename' => 'document.txt', 'full_path' => '/home/user/document.txt'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_building_sql_queries() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['select_part' => 'SELECT * FROM ', 'table_name' => 'users'],
                        ['select_part' => 'SELECT name FROM ', 'table_name' => 'products'],
                    ]
                )
            )
            ->withEntry('query', ref('table_name')->prepend(ref('select_part')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['select_part' => 'SELECT * FROM ', 'table_name' => 'users', 'query' => 'SELECT * FROM users'],
                ['select_part' => 'SELECT name FROM ', 'table_name' => 'products', 'query' => 'SELECT name FROM products'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_building_urls() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['protocol' => 'https://', 'domain' => 'api.example.com'],
                        ['protocol' => 'http://', 'domain' => 'localhost:8080'],
                    ]
                )
            )
            ->withEntry('full_url', ref('domain')->prepend(ref('protocol')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['protocol' => 'https://', 'domain' => 'api.example.com', 'full_url' => 'https://api.example.com'],
                ['protocol' => 'http://', 'domain' => 'localhost:8080', 'full_url' => 'http://localhost:8080'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_multiple_operations() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['part1' => 'Hello', 'part2' => ' beautiful', 'part3' => ' world'],
                    ]
                )
            )
            ->withEntry('result', ref('part3')->prepend(ref('part2'))->prepend(ref('part1')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['part1' => 'Hello', 'part2' => ' beautiful', 'part3' => ' world', 'result' => 'Hello beautiful world'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_with_empty_strings() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['prefix' => '', 'content' => 'main'],
                        ['prefix' => 'pre_', 'content' => ''],
                        ['prefix' => '', 'content' => ''],
                    ]
                )
            )
            ->withEntry('result', ref('content')->prepend(ref('prefix')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['prefix' => '', 'content' => 'main', 'result' => 'main'],
                ['prefix' => 'pre_', 'content' => '', 'result' => 'pre_'],
                ['prefix' => '', 'content' => '', 'result' => ''],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_with_null_values() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => null],
                        ['name' => 'Smith'],
                    ]
                )
            )
            ->withEntry('full_name', ref('name')->prepend(lit('Dr. ')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => null, 'full_name' => null],
                ['name' => 'Smith', 'full_name' => 'Dr. Smith'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_with_scalar_function_prefix() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['title' => 'Mr. ', 'last' => 'Johnson'],
                        ['title' => 'Ms. ', 'last' => 'Williams'],
                    ]
                )
            )
            ->withEntry('full_name', ref('last')->prepend(ref('title')))
            ->drop('title')
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['last' => 'Johnson', 'full_name' => 'Mr. Johnson'],
                ['last' => 'Williams', 'full_name' => 'Ms. Williams'],
            ],
            $memory->dump()
        );
    }

    public function test_prepend_with_unicode_content() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['name' => 'दुनिया'],
                    ]
                )
            )
            ->withEntry('complete_greeting', ref('name')->prepend(lit('नमस्ते ')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['name' => 'दुनिया', 'complete_greeting' => 'नमस्ते दुनिया'],
            ],
            $memory->dump()
        );
    }
}
