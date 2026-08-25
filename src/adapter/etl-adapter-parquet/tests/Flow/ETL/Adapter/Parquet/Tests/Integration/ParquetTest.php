<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SizeUnits;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid;

use function array_keys;
use function extension_loaded;
use function file_exists;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function unlink;

final class ParquetTest extends FlowTestCase
{
    public function test_writing_and_reading_into_parquet(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/file.snappy.parquet');

        $config = config();
        data_frame($config)
            ->read(new FakeExtractor(10))
            ->drop('null', 'array', 'object', 'enum')
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        static::assertEquals(10, data_frame($config)->read(from_parquet($path, filesystem: $memory))->count());
    }

    public function test_writing_and_reading_with_explicit_arrow_engine(): void
    {
        if (!extension_loaded('arrow')) {
            static::markTestSkipped('arrow extension is not loaded');
        }

        $memory = memory_filesystem();
        $path = path('memory://var/arrow_engine.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_parquet($path, engine: new ArrowParquetEngine(), filesystem: $memory))
            ->run();

        static::assertSame(
            2,
            data_frame($config)->read(from_parquet(
                $path,
                engine: new ArrowParquetEngine(),
                filesystem: $memory,
            ))->count(),
        );
    }

    public function test_writing_and_reading_with_explicit_php_engine(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/php_engine.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_parquet($path, engine: new PhpParquetEngine(), filesystem: $memory))
            ->run();

        static::assertSame(
            2,
            data_frame($config)->read(from_parquet(
                $path,
                engine: new PhpParquetEngine(),
                filesystem: $memory,
            ))->count(),
        );
    }

    public function test_round_trip_of_inferred_nested_arrays_projected_to_json(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/inferred_nested_arrays.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([
                ['body' => ['data' => [1, 'a'], 'id' => 1]],
                ['body' => ['data' => [], 'id' => 2]],
            ]))
            ->collect()
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $rows = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch();

        static::assertSame('structure{data: json, id: integer}', $rows->schema()->get('body')->type()->toString());

        $first = $rows[0]->valueOf('body');
        static::assertIsArray($first);
        static::assertInstanceOf(Json::class, $first['data']);
        static::assertSame([1, 'a'], $first['data']->toArray());

        $second = $rows[1]->valueOf('body');
        static::assertIsArray($second);
        static::assertInstanceOf(Json::class, $second['data']);
        static::assertSame([], $second['data']->toArray());
    }

    public function test_round_trip_of_nested_json_values(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/nested_json.parquet');
        $config = config();

        data_frame($config)
            ->read(from_rows(rows(row(
                struct_entry('body', ['data' => Json::fromArray(['a' => 1]), 'n' => 1], type_structure([
                    'data' => type_json(),
                    'n' => type_integer(),
                ])),
                list_entry('json_list', [Json::fromArray(['c' => 3])], type_list(type_json())),
                struct_entry('outer', ['inner' => ['deep' => Json::fromArray(['e' => 5])]], type_structure([
                    'inner' => type_structure(['deep' => type_json()]),
                ])),
            ))))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $row = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch()[0];

        $body = $row->valueOf('body');
        static::assertIsArray($body);
        static::assertInstanceOf(Json::class, $body['data']);
        static::assertSame(['a' => 1], $body['data']->toArray());
        static::assertSame(1, $body['n']);

        $jsonList = $row->valueOf('json_list');
        static::assertIsArray($jsonList);
        static::assertInstanceOf(Json::class, $jsonList[0]);
        static::assertSame(['c' => 3], $jsonList[0]->toArray());

        $outer = $row->valueOf('outer');
        static::assertIsArray($outer);
        // @mago-ignore analysis:mixed-assignment
        $inner = $outer['inner'];
        static::assertIsArray($inner);
        static::assertInstanceOf(Json::class, $inner['deep']);
        static::assertSame(['e' => 5], $inner['deep']->toArray());
    }

    public function test_round_trip_of_nested_uuid_values(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/nested_uuid.parquet');
        $config = config();
        $uuid = 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a';

        data_frame($config)
            ->read(from_rows(rows(row(
                struct_entry('body', ['id' => FlowUuid::fromString($uuid), 'n' => 1], type_structure([
                    'id' => type_uuid(),
                    'n' => type_integer(),
                ])),
                list_entry('uuid_list', [FlowUuid::fromString($uuid)], type_list(type_uuid())),
                struct_entry('outer', ['inner' => ['id' => FlowUuid::fromString($uuid)]], type_structure([
                    'inner' => type_structure(['id' => type_uuid()]),
                ])),
            ))))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $row = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch()[0];

        $body = $row->valueOf('body');
        static::assertIsArray($body);
        static::assertInstanceOf(FlowUuid::class, $body['id']);
        static::assertSame($uuid, $body['id']->toString());

        $uuidList = $row->valueOf('uuid_list');
        static::assertIsArray($uuidList);
        static::assertInstanceOf(FlowUuid::class, $uuidList[0]);

        $outer = $row->valueOf('outer');
        static::assertIsArray($outer);
        // @mago-ignore analysis:mixed-assignment
        $inner = $outer['inner'];
        static::assertIsArray($inner);
        static::assertInstanceOf(FlowUuid::class, $inner['id']);
        static::assertSame($uuid, $inner['id']->toString());
    }

    public function test_round_trip_of_json_and_uuid_map_values(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/logical_maps.parquet');
        $config = config();
        $uuid = 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a';

        data_frame($config)
            ->read(from_rows(rows(row(
                map_entry('uuid_map', ['k' => FlowUuid::fromString($uuid)], type_map(type_string(), type_uuid())),
                map_entry('json_map', ['k' => Json::fromArray(['d' => 4])], type_map(type_string(), type_json())),
            ))))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $row = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch()[0];

        $uuidMap = $row->valueOf('uuid_map');
        static::assertIsArray($uuidMap);
        static::assertSame(['k'], array_keys($uuidMap));
        static::assertInstanceOf(FlowUuid::class, $uuidMap['k']);
        static::assertSame($uuid, $uuidMap['k']->toString());

        $jsonMap = $row->valueOf('json_map');
        static::assertIsArray($jsonMap);
        static::assertSame(['k'], array_keys($jsonMap));
        static::assertInstanceOf(Json::class, $jsonMap['k']);
        static::assertSame(['d' => 4], $jsonMap['k']->toArray());
    }

    public function test_writing_and_reading_parquet_orders(): void
    {
        $path = path(__DIR__ . '/var/orders.snappy.parquet');
        $config = config();
        data_frame($config)
            ->read(new FakeRandomOrdersExtractor(1000))
            ->write(
                to_parquet($path)
                    ->saveMode(overwrite())
                    ->withOptions(Options::default()->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 500)->set(
                        Option::ROW_GROUP_SIZE_BYTES,
                        SizeUnits::MiB_SIZE,
                    )->set(Option::PAGE_MAXIMUM_ROWS_COUNT, 10)),
            )
            ->run();

        static::assertEquals(1000, data_frame($config)->read(from_parquet($path))->count());
    }

    public function test_writing_with_provided_schema(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/file_schema.snappy.parquet');
        $config = config();
        data_frame($config)
            ->read(from_array([
                [
                    'id' => 1,
                    'name' => 'test',
                    'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'),
                    'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]',
                ],
                [
                    'id' => 2,
                    'name' => 'test',
                    'uuid' => Uuid::fromString('26fd21b0-6080-4d6c-bdb4-1214f1feffef'),
                    'json' => '[{"id":1,"name":"test"},{"id":2,"name":"test"}]',
                ],
            ]))
            ->write(to_parquet(
                $path,
                filesystem: $memory,
                schema: schema(str_schema('id'), str_schema('name'), str_schema('uuid'), json_schema('json')),
            ))
            ->run();

        static::assertEquals(
            [
                [
                    'id' => '1',
                    'name' => 'test',
                    'uuid' => '26fd21b0-6080-4d6c-bdb4-1214f1feffef',
                    'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']],
                ],
                [
                    'id' => '2',
                    'name' => 'test',
                    'uuid' => '26fd21b0-6080-4d6c-bdb4-1214f1feffef',
                    'json' => [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']],
                ],
            ],
            data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch()->toArray(),
        );

        static::assertTrue($memory->status($path)?->isFile());
    }

    public function test_transformation_loader_writes_all_batches_to_parquet(): void
    {
        data_frame()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_parquet($path = __DIR__ . '/var/test_transformation_loader.parquet')->saveMode(overwrite()),
            ))
            ->run();

        $rows = data_frame()->read(from_parquet($path))->fetch();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows->schema()->count());

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
