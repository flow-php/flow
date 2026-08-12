<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeEngine;
use Flow\Floe\NativeFloeEncoder;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;

final class FloeDataFrameTest extends FlowIntegrationTestCase
{
    public function test_append_writes_sibling_files_read_as_union(): void
    {
        $dir = $this->cacheDir->path() . '/appended';

        data_frame()
            ->read(from_array([['id' => 1]]))
            ->saveMode(overwrite())
            ->write(to_floe($dir . '/data.floe'))
            ->run();

        data_frame()
            ->read(from_array([['id' => 2]]))
            ->saveMode(append())
            ->write(to_floe($dir . '/data.floe'))
            ->run();

        static::assertSame(2, data_frame()->read(from_floe($dir . '/*.floe'))->count());
    }

    public function test_writing_and_reading_with_explicit_native_engine(): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension is not loaded');
        }

        $path = $this->cacheDir->suffix('native-engine.floe');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->saveMode(overwrite())
            ->write(to_floe($path, engine: FloeEngine::native))
            ->run();

        static::assertSame(2, data_frame()->read(from_floe($path, engine: FloeEngine::native))->count());
    }

    public function test_writing_and_reading_with_explicit_php_engine(): void
    {
        $path = $this->cacheDir->suffix('php-engine.floe');

        data_frame(config_builder()->hydrator(new PhpRowHydrator()))
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->saveMode(overwrite())
            ->write(to_floe($path, engine: FloeEngine::php))
            ->run();

        static::assertSame(
            2,
            data_frame(config_builder()->hydrator(new PhpRowHydrator()))
                ->read(from_floe($path, engine: FloeEngine::php))
                ->count(),
        );
    }

    public function test_inferred_nested_arrays_are_projected_to_json_and_round_trip(): void
    {
        $path = $this->cacheDir->suffix('nested-arrays.floe');

        data_frame()
            ->read(from_array([
                ['body' => ['data' => [1, 'a'], 'id' => 1]],
                ['body' => ['data' => [], 'id' => 2]],
            ]))
            ->collect()
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        $rows = data_frame()->read(from_floe($path))->fetch();

        static::assertSame('structure{data: json, id: integer}', $rows->schema()->get('body')->type()->toString());
        static::assertFalse($rows->schema()->get('body')->isNullable());

        $first = $rows[0]->valueOf('body');
        static::assertIsArray($first);
        static::assertInstanceOf(Json::class, $first['data']);
        static::assertSame([1, 'a'], $first['data']->toArray());

        $second = $rows[1]->valueOf('body');
        static::assertIsArray($second);
        static::assertInstanceOf(Json::class, $second['data']);
        static::assertSame([], $second['data']->toArray());
    }

    public function test_list_of_json_round_trip(): void
    {
        $path = $this->cacheDir->suffix('list-json.floe');

        data_frame()
            ->read(from_rows(rows(row(list_entry(
                'json_list',
                [Json::fromArray(['a' => 1]), Json::fromArray([1, 'b'])],
                type_list(type_json()),
            )))))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        $rows = data_frame()->read(from_floe($path))->fetch();

        static::assertSame('list<json>', $rows->schema()->get('json_list')->type()->toString());

        $list = $rows[0]->valueOf('json_list');
        static::assertIsArray($list);
        static::assertInstanceOf(Json::class, $list[0]);
        static::assertSame(['a' => 1], $list[0]->toArray());
        static::assertInstanceOf(Json::class, $list[1]);
        static::assertSame([1, 'b'], $list[1]->toArray());
    }

    public function test_input_file_uri_is_added_when_configured(): void
    {
        $path = $this->cacheDir->suffix('input-uri.floe');

        data_frame()
            ->read(from_array([['id' => 1]]))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        $rows = data_frame(config_builder()->putInputIntoRows())->read(from_floe($path))->fetch();

        static::assertTrue($rows->first()->entries()->has('_input_file_uri'));
    }

    public function test_offset_and_limit_pushdown(): void
    {
        $path = $this->cacheDir->suffix('pushdown.floe');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        static::assertSame(2, data_frame()->read(from_floe($path))->limit(2)->fetch()->count());
        static::assertSame(2, data_frame()->read(from_floe($path)->withOffset(3))->fetch()->count());
    }

    public function test_overwrite_replaces_the_dataset(): void
    {
        $path = $this->cacheDir->suffix('overwrite.floe');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        data_frame()
            ->read(from_array([['id' => 9]]))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        static::assertSame(1, data_frame()->read(from_floe($path))->count());
    }

    public function test_partitioned_round_trip_with_pruning(): void
    {
        $dir = $this->cacheDir->path() . '/parts';

        data_frame()
            ->read(from_array([
                ['id' => 1, 'country' => 'PL'],
                ['id' => 2, 'country' => 'US'],
                ['id' => 3, 'country' => 'PL'],
            ]))
            ->partitionBy('country')
            ->saveMode(overwrite())
            ->write(to_floe($dir . '/data.floe'))
            ->run();

        static::assertFileExists($dir . '/country=PL/data.floe');
        static::assertFileExists($dir . '/country=US/data.floe');

        static::assertSame(3, data_frame()->read(from_floe($dir . '/country=*/data.floe'))->count());
        static::assertSame(2, data_frame()->read(from_floe($dir . '/country=PL/data.floe'))->count());
    }

    public function test_round_trip(): void
    {
        $path = $this->cacheDir->suffix('roundtrip.floe');

        data_frame()
            ->read(from_array([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]))
            ->saveMode(overwrite())
            ->write(to_floe($path))
            ->run();

        $result = data_frame()->read(from_floe($path))->fetch();

        static::assertSame(2, $result->count());
        static::assertSame(['id', 'name'], $result->first()->entries()->names());
    }
}
