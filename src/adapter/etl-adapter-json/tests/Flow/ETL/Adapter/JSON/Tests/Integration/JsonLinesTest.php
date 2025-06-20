<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use function Flow\ETL\Adapter\JSON\{from_json_lines, to_json_lines};
use function Flow\ETL\DSL\{config, flow_context, rows};
use function Flow\ETL\DSL\{df, from_array, overwrite, ref};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Adapter\JSON\JsonLinesLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\{Tests\FlowTestCase};

final class JsonLinesTest extends FlowTestCase
{
    public function test_ignores_pretty() : void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
                ['name' => 'Jake', 'age' => 30],
                ['name' => 'Joe', 'age' => 30],
            ]))
            ->saveMode(overwrite())
            ->write(to_json_lines($path = __DIR__ . '/var/test_jsonl_ignore_pretty.jsonl')->withFlags(JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES))
            ->run();

        $content = \file_get_contents($path);

        if ($content === false) {
            self::fail('Failed to read file content');
        }

        self::assertStringContainsString(
            <<<'JSON'
{"name":"John","age":30}
{"name":"Jane","age":25}
{"name":"Jake","age":30}
{"name":"Joe","age":30}
JSON,
            $content
        );
    }

    public function test_jsonl_loader() : void
    {
        $path = __DIR__ . '/var/test_json_loader.jsonl';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json_lines($path))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_json_lines($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_jsonl_loader_loading_empty_string() : void
    {
        $loader = new JsonLinesLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.jsonl'));

        $loader->load(rows(), $context = flow_context(config()));

        $loader->closure($context);

        $content = \file_get_contents($path);
        self::assertEmpty($content);

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_jsonl_loader_overwrite_mode() : void
    {
        $path = __DIR__ . '/var/test_jsonl_loader.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()
            ->read(new FakeExtractor(100))
            ->write(to_json_lines($path))
            ->run();

        df()
            ->read(new FakeExtractor(100))
            ->mode(overwrite())
            ->write(to_json_lines($path))
            ->run();

        self::assertEquals(
            100,
            df()->read(from_json_lines($path))->count()
        );

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_partitioning_jsonl_file() : void
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
            ->write(to_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/products.jsonl'))
            ->run();

        self::assertEquals(
            $dataset,
            df()
                ->read(from_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/**/*.jsonl'))
                ->sortBy(ref('id')->asc())
                ->fetch()
                ->toArray()
        );
    }
}
