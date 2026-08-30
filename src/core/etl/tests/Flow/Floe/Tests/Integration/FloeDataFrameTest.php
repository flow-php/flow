<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use DateTimeZone;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeEngine;
use Flow\Floe\NativeFloeEncoder;
use Flow\Types\Value\Json;

use function array_keys;
use function Flow\ETL\DSL\append;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_time_zone;

final class FloeDataFrameTest extends FlowIntegrationTestCase
{
    public function test_append_writes_sibling_files_read_as_union(): void
    {
        $dir = $this->cacheDir->path() . '/appended';

        data_frame()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($dir . '/data.floe')->saveMode(overwrite()))
            ->run();

        data_frame()
            ->read(from_array([['id' => 2]]))
            ->write(to_floe($dir . '/data.floe')->saveMode(append()))
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
            ->write(to_floe($path, engine: FloeEngine::native)->saveMode(overwrite()))
            ->run();

        static::assertSame(2, data_frame()->read(from_floe($path, engine: FloeEngine::native))->count());
    }

    public function test_writing_and_reading_with_explicit_php_engine(): void
    {
        $path = $this->cacheDir->suffix('php-engine.floe');

        data_frame(config_builder()->hydrator(new PhpRowHydrator()))
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_floe($path, engine: FloeEngine::php)->saveMode(overwrite()))
            ->run();

        static::assertSame(
            2,
            data_frame(config_builder()->hydrator(new PhpRowHydrator()))
                ->read(from_floe($path, engine: FloeEngine::php))
                ->count(),
        );
    }

    public function test_a_timezone_column_round_trips_through_floe(): void
    {
        $path = $this->cacheDir->suffix('timezones.floe');

        data_frame()
            ->read(from_rows(rows(
                schema(time_zone_schema('tz')),
                row(['tz' => type_time_zone()->cast('Europe/Warsaw')]),
                row(['tz' => type_time_zone()->cast('UTC')]),
            )))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        $read = data_frame()->read(from_floe($path))->fetch();

        static::assertSame('timezone', $read->schema()->get('tz')->type()->toString());

        $names = [];

        foreach ($read as $row) {
            $names[] = type_instance_of(DateTimeZone::class)->assert($row->get('tz'))->getName();
        }

        static::assertSame(['Europe/Warsaw', 'UTC'], $names);
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
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        $rows = data_frame()->read(from_floe($path))->fetch();

        static::assertSame('structure{data: json, id: integer}', $rows->schema()->get('body')->type()->toString());
        // NOT NULL declaration.
        static::assertFalse($rows->schema()->get('body')->isNullable());

        $first = $rows[0]->get('body');
        static::assertIsArray($first);
        static::assertInstanceOf(Json::class, $first['data']);
        static::assertSame([1, 'a'], $first['data']->toArray());

        $second = $rows[1]->get('body');
        static::assertIsArray($second);
        static::assertInstanceOf(Json::class, $second['data']);
        static::assertSame([], $second['data']->toArray());
    }

    public function test_list_of_json_round_trip(): void
    {
        $path = $this->cacheDir->suffix('list-json.floe');

        data_frame()
            ->read(from_rows(rows(
                schema(list_schema('json_list', type_list(type_json()))),
                row(['json_list' => [Json::fromArray(['a' => 1]), Json::fromArray([1, 'b'])]]),
            )))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        $rows = data_frame()->read(from_floe($path))->fetch();

        static::assertSame('list<json>', $rows->schema()->get('json_list')->type()->toString());

        $list = $rows[0]->get('json_list');
        static::assertIsArray($list);
        static::assertInstanceOf(Json::class, $list[0]);
        static::assertSame(['a' => 1], $list[0]->toArray());
        static::assertInstanceOf(Json::class, $list[1]);
        static::assertSame([1, 'b'], $list[1]->toArray());
    }

    /**
     * schema() used to never declare the column extract() adds, so the two disagreed.
     */
    public function test_schema_and_extract_agree_in_both_metadata_states(): void
    {
        $path = $this->cacheDir->suffix('metadata-agreement.floe');

        data_frame()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        foreach ([false, true] as $enabled) {
            $extractor = from_floe($path)->withMetadataColumns($enabled);

            static::assertSame(
                array_keys($extractor->schema()->definitions()),
                array_keys(data_frame()->read($extractor)->fetch()->first()->toArray()),
            );
        }
    }

    public function test_input_file_uri_is_added_when_configured(): void
    {
        $path = $this->cacheDir->suffix('input-uri.floe');

        data_frame()
            ->read(from_array([['id' => 1]]))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        $rows = data_frame()->read(from_floe($path)->withMetadataColumns(true))->fetch();

        static::assertTrue($rows->first()->has('_input_file_uri'));
    }

    public function test_offset_and_limit_pushdown(): void
    {
        $path = $this->cacheDir->suffix('pushdown.floe');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        static::assertSame(2, data_frame()->read(from_floe($path))->limit(2)->fetch()->count());
        static::assertSame(2, data_frame()->read(from_floe($path)->withOffset(3))->fetch()->count());
    }

    public function test_overwrite_replaces_the_dataset(): void
    {
        $path = $this->cacheDir->suffix('overwrite.floe');

        data_frame()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        data_frame()
            ->read(from_array([['id' => 9]]))
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        static::assertSame(1, data_frame()->read(from_floe($path))->count());
    }

    public function test_transformation_loader_writes_all_batches_to_floe(): void
    {
        $path = $this->cacheDir->suffix('transformation.floe');

        data_frame()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(select('id'), to_floe($path)->saveMode(overwrite())))
            ->run();

        $rows = data_frame()->read(from_floe($path))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());
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
            ->write(to_floe($dir . '/data.floe')->saveMode(overwrite()))
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
            ->write(to_floe($path)->saveMode(overwrite()))
            ->run();

        $result = data_frame()->read(from_floe($path))->fetch();

        static::assertSame(2, $result->count());
        static::assertSame(['id', 'name'], $result->first()->names());
    }
}
