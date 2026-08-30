<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use DOMDocument;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Json\to_json;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_json;
use function unlink;

final class JsonTest extends FlowTestCase
{
    public function test_domdocument_json_file(): void
    {
        $domDocument = new DOMDocument();
        $domDocument->loadXml('<b>red</b>');

        df()
            ->read(from_array([
                ['id' => 1, 'descriptionHtml' => $domDocument, 'size' => 'small'],
            ]))
            ->write(to_json($path = __DIR__ . '/var/test_domdocument.json')->saveMode(overwrite()))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            [{"id":1,"descriptionHtml":"<b>red<\/b>","size":"small"}]
            JSON, $content);
    }

    public function test_json_loader(): void
    {
        $path = __DIR__ . '/var/test_json_loader.json';

        if (file_exists($path)) {
            unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json($path))->run();

        static::assertEquals(100, df()->read(from_json($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_json_loader_loading_empty_string(): void
    {
        $loader = new JsonLoader(path($path = __DIR__ . '/var/test_json_loader_loading_empty_string.json'));

        $loader->load(rows(schema()), $context = flow_context(config()));

        $loader->closure($context);

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertJsonStringEqualsJsonString(<<<'JSON'
            [
            ]
            JSON, $content);

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_json_loader_overwrite_mode(): void
    {
        $path = __DIR__ . '/var/test_json_loader.json';

        if (file_exists($path)) {
            unlink($path);
        }

        df()->read(new FakeExtractor(100))->write(to_json($path))->run();

        df()->read(new FakeExtractor(100))->write(to_json($path)->saveMode(overwrite()))->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringEndsWith(']', $content);

        static::assertEquals(100, df()->read(from_json($path))->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function test_jsonentry_json_file(): void
    {
        $jsonObject = ['short' => 'short_description', 'long' => 'long_description'];
        df()
            ->read(from_rows(rows(
                schema(int_schema('id'), json_schema('nested')),
                row(['id' => 1, 'nested' => type_json()->cast($jsonObject)]),
            )))
            ->write(to_json($path = __DIR__ . '/var/test_jsonentry.json')->saveMode(overwrite()))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            [{"id":1,"nested":{"short":"short_description","long":"long_description"}}]
            JSON, $content);
    }

    public function test_list_of_structures_json_file(): void
    {
        df()
            ->read(from_array([
                ['id' => 2, 'tags' => [['t' => 'a'], ['t' => 'b']]],
            ]))
            ->write(to_json($path = __DIR__ . '/var/test_list_of_structures.json')->saveMode(overwrite()))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            [{"id":2,"tags":[{"t":"a"},{"t":"b"}]}]
            JSON, $content);
    }

    public function test_partitioning_json_file(): void
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
            ->partitionBy('size', 'color')
            ->write(to_json(__DIR__ . '/var/test_partitioning_json_file/products.json')->saveMode(overwrite()))
            ->run();

        static::assertEquals(
            $dataset,
            df()
                ->read(from_json(__DIR__ . '/var/test_partitioning_json_file/**/*.json'))
                ->sortBy([ref('id')->asc()])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_putting_each_row_in_a_new_line(): void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30],
                ['name' => 'Jane', 'age' => 25],
                ['name' => 'Jake', 'age' => 30],
                ['name' => 'Joe', 'age' => 30],
            ]))
            ->write(
                to_json(
                    $path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json',
                    put_rows_in_new_lines: true,
                )->saveMode(overwrite()),
            )
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            [
            {"name":"John","age":30},
            {"name":"Jane","age":25},
            {"name":"Jake","age":30},
            {"name":"Joe","age":30}
            ]
            JSON, $content);
    }

    public function test_putting_each_row_in_a_new_line_with_json_pretty_print_flag(): void
    {
        df()
            ->read(from_array([
                ['name' => 'John', 'age' => 30, 'pets' => 1],
                ['name' => 'Jane', 'age' => 25, 'pets' => 3],
                ['name' => 'Jake', 'age' => 30, 'pets' => 1],
                ['name' => 'Joe', 'age' => 30, 'pets' => 0],
            ]))
            ->groupBy(['age'])
            ->aggregate(average(ref('pets')))
            ->write(
                to_json(
                    $path = __DIR__ . '/var/test_putting_each_row_in_a_new_line.json',
                    flags: JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT,
                    put_rows_in_new_lines: true,
                )->saveMode(overwrite()),
            )
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringContainsString(<<<'JSON'
            [
            {
                "age": 30,
                "pets_avg": 0.67
            },
            {
                "age": 25,
                "pets_avg": 3
            }
            ]
            JSON, $content);
    }

    public function test_transformation_loader_writes_parseable_json_across_batches(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_json($path = __DIR__ . '/var/test_transformation_loader.json')->saveMode(overwrite()),
            ))
            ->run();

        $content = file_get_contents($path);

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertJson($content);

        $rows = df()->read(from_json($path))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
