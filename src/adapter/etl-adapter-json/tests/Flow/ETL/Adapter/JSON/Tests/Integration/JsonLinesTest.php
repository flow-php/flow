<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JsonLinesLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path;

final class JsonLinesTest extends FlowTestCase
{
    public function test_ignores_pretty(): void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
                ['name' => 'Jake', 'age' => 30],
                ['name' => 'Joe', 'age' => 30],
            ]))
            ->saveMode(overwrite())
            ->write(
                to_json_lines($path = __DIR__ . '/var/test_jsonl_ignore_pretty.jsonl')
                    ->withFlags(JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES),
            )
            ->run();

        $content = \file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            {"name":"John","age":30}
            {"name":"Jane","age":25}
            {"name":"Jake","age":30}
            {"name":"Joe","age":30}
            JSON, $content);
    }

    public function test_jsonl_loader(): void
    {
        $path = __DIR__ . '/var/test_json_loader.jsonl';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json_lines($path))->run();

        static::assertEquals(100, df()->read(from_json_lines($path))->count());

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_jsonl_loader_loading_empty_string(): void
    {
        $loader = new JsonLinesLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.jsonl'));

        $loader->load(rows(), $context = flow_context(config()));

        $loader->closure($context);

        $content = \file_get_contents($path);
        static::assertEmpty($content);

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_jsonl_loader_overwrite_mode(): void
    {
        $path = __DIR__ . '/var/test_jsonl_loader.json';

        if (\file_exists($path)) {
            \unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json_lines($path))->run();

        df()->read(new FakeExtractor(100))->mode(overwrite())->write(to_json_lines($path))->run();

        static::assertEquals(100, df()->read(from_json_lines($path))->count());

        if (\file_exists($path)) {
            \unlink($path);
        }
    }

    public function test_partitioning_jsonl_file(): void
    {
        df()
            ->read(from_array(
                $dataset = [
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
                ],
            ))
            ->saveMode(overwrite())
            ->partitionBy('size', 'color')
            ->write(to_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/products.jsonl'))
            ->run();

        static::assertEquals(
            $dataset,
            df()
                ->read(from_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/**/*.jsonl'))
                ->sortBy(ref('id')->asc())
                ->fetch()
                ->toArray(),
        );
    }
}
