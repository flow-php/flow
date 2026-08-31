<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JsonLinesLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Filesystem\DSL\path;
use function unlink;

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
            ->write(
                to_json_lines($path = __DIR__ . '/var/test_jsonl_ignore_pretty.jsonl')
                    ->saveMode(overwrite())
                    ->withFlags(JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES),
            )
            ->run();

        $content = file_get_contents($path);

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

        if (file_exists($path)) {
            unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json_lines($path))->run();

        static::assertEquals(100, df()->read(from_json_lines($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_jsonl_loader_loading_empty_string(): void
    {
        $loader = new JsonLinesLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.jsonl'));

        $loader->load(rows(schema()), $context = flow_context(config()));

        $loader->closure($context);

        $content = file_get_contents($path);
        static::assertEmpty($content);

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_jsonl_loader_overwrite_mode(): void
    {
        $path = __DIR__ . '/var/test_jsonl_loader.json';

        if (file_exists($path)) {
            unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json_lines($path))->run();

        df()->read(new FakeExtractor(100))->write(to_json_lines($path)->saveMode(overwrite()))->run();

        static::assertEquals(100, df()->read(from_json_lines($path))->count());

        if (file_exists($path)) {
            unlink($path);
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
            ->write(
                to_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/products.jsonl')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('size', 'color')),
            )
            ->run();

        static::assertEquals(
            $dataset,
            df()
                ->read(from_json_lines(__DIR__ . '/var/test_partitioning_jsonl_file/**/*.jsonl'))
                ->sortBy([ref('id')->asc()])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_transformation_loader_writes_all_batches_to_json_lines(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_json_lines($path = __DIR__ . '/var/test_transformation_loader.jsonl')->saveMode(overwrite()),
            ))
            ->run();

        $rows = df()->read(from_json_lines($path))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
