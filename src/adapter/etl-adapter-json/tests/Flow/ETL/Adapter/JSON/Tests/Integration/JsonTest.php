<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\{df, from_array, overwrite};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, FlowContext, Rows};
use PHPUnit\Framework\TestCase;

final class JsonTest extends TestCase
{
    public function test_json_loader() : void
    {

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_json($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_json_loader_loading_empty_string() : void
    {
        $loader = new JsonLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.json'));

        $loader->load(new Rows(), $context = new FlowContext(Config::default()));

        $loader->closure($context);

        self::assertJsonStringEqualsJsonString(
            <<<'JSON'
[
]
JSON,
            \file_get_contents($path)
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_json_loader_overwrite_mode() : void
    {

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
            ->run();

        df()
            ->read(new FakeExtractor(100))
            ->mode(overwrite())
            ->write(to_json($path = __DIR__ . '/var/test_json_loader.json'))
            ->run();

        $content = \file_get_contents($path);
        self::stringEndsWith(']', $content);

        self::assertEquals(
            100,
            df()->read(from_json($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_putting_each_row_in_a_new_line() : void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
            ]))
            ->saveMode(overwrite())
            ->write(to_json($path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json', put_rows_in_new_lines: true))
            ->run();

        self::assertStringContainsString(
            <<<'JSON'
[
{"name":"John","age":30},
{"name":"Jane","age":25}
]
JSON,
            \file_get_contents($path)
        );
    }

    public function test_putting_each_row_in_a_new_line_with_json_pretty_print_flag() : void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
            ]))
            ->saveMode(overwrite())
            ->write(to_json($path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json', flags: JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT, put_rows_in_new_lines: true))
            ->run();

        self::assertStringContainsString(
            <<<'JSON'
[
{
    "name": "John",
    "age": 30
},
{
    "name": "Jane",
    "age": 25
}
]
JSON,
            \file_get_contents($path)
        );
    }
}
