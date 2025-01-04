<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\{average, df, from_array, overwrite, ref};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Config, FlowContext, Rows, Tests\FlowTestCase};

final class JsonTest extends FlowTestCase
{
    public function test_json_loader() : void
    {
        $path = __DIR__ . '/var/test_json_loader.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path))
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
        $path = __DIR__ . '/var/test_json_loader.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json($path))
            ->run();

        df()
            ->read(new FakeExtractor(100))
            ->mode(overwrite())
            ->write(to_json($path))
            ->run();

        $content = \file_get_contents($path);
        self::assertStringEndsWith(']', $content);

        self::assertEquals(
            100,
            df()->read(from_json($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_partitioning_json_file() : void
    {
        df()
            ->read(from_array($dataset = [
                ['id' => 1, 'color' => 'red', 'size' => 'small'],
                ['id' => 2, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 3, 'color' => 'green', 'size' => 'large'],
                ['id' => 4, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 5, 'color' => 'black', 'size' => 'medium'],
                ['id' => 6, 'color' => 'white', 'size' => 'large'],
                ['id' => 7, 'color' => 'red', 'size' => 'small'],
                ['id' => 8, 'color' => 'blue', 'size' => 'medium'],
                ['id' => 9, 'color' => 'green', 'size' => 'large'],
                ['id' => 10, 'color' => 'yellow', 'size' => 'small'],
                ['id' => 11, 'color' => 'black', 'size' => 'medium'],
                ['id' => 12, 'color' => 'white', 'size' => 'large'],
            ]))
            ->saveMode(overwrite())
            ->partitionBy('size', 'color')
            ->write(to_json($path = __DIR__ . '/var/test_partitioning_json_file/products.json'))
            ->run();

        self::assertEquals(
            $dataset,
            df()
                ->read(from_json(__DIR__ . '/var/test_partitioning_json_file/**/*.json'))
                ->sortBy(ref('id')->asc())
                ->fetch()
                ->toArray()
        );
    }

    public function test_putting_each_row_in_a_new_line() : void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
                ['name' => 'Jake', 'age' => 30],
                ['name' => 'Joe', 'age' => 30],
            ]))
            ->saveMode(overwrite())
            ->write(to_json($path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json', put_rows_in_new_lines: true))
            ->run();

        self::assertStringContainsString(
            <<<'JSON'
[
{"name":"John","age":30},
{"name":"Jane","age":25},
{"name":"Jake","age":30},
{"name":"Joe","age":30}
]
JSON,
            \file_get_contents($path)
        );
    }

    public function test_putting_each_row_in_a_new_line_with_json_pretty_print_flag() : void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30, 'pets' => 1],
                ['name' => 'Jane', 'age' => 25, 'pets' => 3],
                ['name' => 'Jake', 'age' => 30, 'pets' => 1],
                ['name' => 'Joe', 'age' => 30, 'pets' => 0],
            ]))
            ->saveMode(overwrite())
            ->groupBy('age')
            ->aggregate(average(ref('pets')))
            ->write(to_json($path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json', flags: JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT, put_rows_in_new_lines: true))
            ->run();

        self::assertStringContainsString(
            <<<'JSON'
[
{
    "age": 30,
    "pets_avg": 0.666667
},
{
    "age": 25,
    "pets_avg": 3
}
]
JSON,
            \file_get_contents($path)
        );
    }
}
