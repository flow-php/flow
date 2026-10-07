<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Tests\Context\LoaderEndingContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Tests\Double\FailingCloseFilesystem;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Exception\RuntimeException as ParquetRuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid as FlowUuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use Ramsey\Uuid\Uuid;

use function array_column;
use function array_keys;
use function array_map;
use function extension_loaded;
use function file_exists;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function iterator_to_array;
use function range;
use function unlink;

final class ParquetTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{?ParquetEngine}>
     */
    public static function engines(): Generator
    {
        yield 'default opener' => [null];
        yield 'php engine' => [new PhpParquetEngine()];
    }

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
            static::markTestSkipped('arrow is not loaded');
        }

        $memory = memory_filesystem();
        $path = path('memory://var/arrow_engine.parquet');
        $config = config();

        data_frame($config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_parquet($path, engine: new RustParquetEngine(), filesystem: $memory))
            ->run();

        static::assertSame(
            2,
            data_frame($config)->read(from_parquet(
                $path,
                engine: new RustParquetEngine(),
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

        // @mago-ignore analysis:mixed-assignment
        $first = $rows->column('body')->value(0);
        static::assertIsArray($first);
        static::assertInstanceOf(Json::class, $first['data']);
        static::assertSame([1, 'a'], $first['data']->toArray());

        // @mago-ignore analysis:mixed-assignment
        $second = $rows->column('body')->value(1);
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
            ->read(from_rows(array_to_rows(
                [[
                    'body' => ['data' => Json::fromArray(['a' => 1]), 'n' => 1],
                    'json_list' => [Json::fromArray(['c' => 3])],
                    'outer' => ['inner' => ['deep' => Json::fromArray(['e' => 5])]],
                ]],
                schema(
                    structure_schema('body', type_structure([
                        'data' => type_json(),
                        'n' => type_integer(),
                    ])),
                    list_schema('json_list', type_list(type_json())),
                    structure_schema('outer', type_structure([
                        'inner' => type_structure(['deep' => type_json()]),
                    ])),
                ),
            )))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $rows = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch();

        // @mago-ignore analysis:mixed-assignment
        $body = $rows->column('body')->value(0);
        static::assertIsArray($body);
        static::assertInstanceOf(Json::class, $body['data']);
        static::assertSame(['a' => 1], $body['data']->toArray());
        static::assertSame(1, $body['n']);

        // @mago-ignore analysis:mixed-assignment
        $jsonList = $rows->column('json_list')->value(0);
        static::assertIsArray($jsonList);
        static::assertInstanceOf(Json::class, $jsonList[0]);
        static::assertSame(['c' => 3], $jsonList[0]->toArray());

        // @mago-ignore analysis:mixed-assignment
        $outer = $rows->column('outer')->value(0);
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
            ->read(from_rows(array_to_rows(
                [[
                    'body' => ['id' => FlowUuid::fromString($uuid), 'n' => 1],
                    'uuid_list' => [FlowUuid::fromString($uuid)],
                    'outer' => ['inner' => ['id' => FlowUuid::fromString($uuid)]],
                ]],
                schema(
                    structure_schema('body', type_structure([
                        'id' => type_uuid(),
                        'n' => type_integer(),
                    ])),
                    list_schema('uuid_list', type_list(type_uuid())),
                    structure_schema('outer', type_structure([
                        'inner' => type_structure(['id' => type_uuid()]),
                    ])),
                ),
            )))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $rows = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch();

        // @mago-ignore analysis:mixed-assignment
        $body = $rows->column('body')->value(0);
        static::assertIsArray($body);
        static::assertInstanceOf(FlowUuid::class, $body['id']);
        static::assertSame($uuid, $body['id']->toString());

        // @mago-ignore analysis:mixed-assignment
        $uuidList = $rows->column('uuid_list')->value(0);
        static::assertIsArray($uuidList);
        static::assertInstanceOf(FlowUuid::class, $uuidList[0]);

        // @mago-ignore analysis:mixed-assignment
        $outer = $rows->column('outer')->value(0);
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
            ->read(from_rows(array_to_rows(
                [[
                    'uuid_map' => ['k' => FlowUuid::fromString($uuid)],
                    'json_map' => ['k' => Json::fromArray(['d' => 4])],
                ]],
                schema(
                    map_schema('uuid_map', type_map(type_string(), type_uuid())),
                    map_schema('json_map', type_map(type_string(), type_json())),
                ),
            )))
            ->write(to_parquet($path, filesystem: $memory))
            ->run();

        $rows = data_frame($config)->read(from_parquet($path, filesystem: $memory))->fetch();

        // @mago-ignore analysis:mixed-assignment
        $uuidMap = $rows->column('uuid_map')->value(0);
        static::assertIsArray($uuidMap);
        static::assertSame(['k'], array_keys($uuidMap));
        static::assertInstanceOf(FlowUuid::class, $uuidMap['k']);
        static::assertSame($uuid, $uuidMap['k']->toString());

        // @mago-ignore analysis:mixed-assignment
        $jsonMap = $rows->column('json_map')->value(0);
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

    #[DataProvider('engines')]
    public function test_writing_with_provided_schema(?ParquetEngine $engine): void
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
                engine: $engine,
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
            data_frame($config)
                ->read(from_parquet($path, filesystem: $memory, engine: $engine))
                ->fetch()
                ->toArray(),
        );

        static::assertTrue($memory->status($path)?->isFile());
    }

    /**
     * Flow float is written as Parquet DOUBLE: no f32 rounding on the way back.
     */
    #[DataProvider('engines')]
    public function test_a_float_round_trips_exactly(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/floats.parquet');

        data_frame()
            ->read(from_rows(array_to_rows([
                ['amount' => 0.14],
                ['amount' => 1234567890123456.8],
            ], schema(float_schema('amount')))))
            ->write(to_parquet($path, filesystem: $memory, engine: $engine))
            ->run();

        static::assertSame(
            [['amount' => 0.14], ['amount' => 1234567890123456.8]],
            data_frame()
                ->read(from_parquet($path, filesystem: $memory, engine: $engine))
                ->fetch()
                ->toArray(),
        );
    }

    #[RequiresPhpExtension('arrow')]
    public function test_arrow_refuses_a_string_that_is_not_valid_utf8(): void
    {
        $this->expectException(ParquetRuntimeException::class);
        $this->expectExceptionMessage(
            'Parquet column "name" row 0 holds a string that is not valid UTF-8; Parquet STRING columns require UTF-8',
        );

        data_frame()
            ->read(from_rows(array_to_rows([['name' => "\xff\xfe"]], schema(str_schema('name')))))
            ->write(to_parquet(path('memory://var/invalid.parquet'), filesystem: memory_filesystem()))
            ->run();
    }

    /**
     * 200 values: a full block and a partial one, both with zero-width miniblocks - read by arrow-rs, not only by PHP.
     */
    #[RequiresPhpExtension('arrow')]
    public function test_a_php_written_delta_binary_packed_column_reads_back_through_arrow(): void
    {
        $memory = memory_filesystem();
        $ids = array_map(static fn(int $i): int => $i * 3, range(0, 199));
        ParquetFilesContext::deltaEncoded($memory, 'memory://var/delta.parquet', $ids);

        static::assertSame(
            array_map(static fn(int $id): array => ['id' => $id], $ids),
            data_frame()
                ->read(from_parquet(path('memory://var/delta.parquet'), filesystem: $memory))
                ->fetch()
                ->toArray(),
        );
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_a_given_engine_is_honoured_with_both_extensions_loaded(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/engine.parquet');

        data_frame()
            ->read(from_rows(array_to_rows([['name' => "\xff\xfe"]], schema(str_schema('name')))))
            ->write(to_parquet(
                $path,
                options: (new Options())->set(Option::VALIDATE_DATA, false),
                engine: new PhpParquetEngine(),
                filesystem: $memory,
            ))
            ->run();

        static::assertSame(
            [['name' => "\xff\xfe"]],
            data_frame()
                ->read(from_parquet($path, filesystem: $memory, engine: new PhpParquetEngine()))
                ->fetch()
                ->toArray(),
        );
    }

    /**
     * @return Generator<string, array{?ParquetEngine}>
     */
    public static function reading_engines(): Generator
    {
        yield 'default engine' => [null];
        yield 'php engine' => [new PhpParquetEngine()];
    }

    #[DataProvider('reading_engines')]
    public function test_a_column_outside_the_projection_never_needs_a_flow_type(?ParquetEngine $engine): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            data_frame()
                ->read(from_parquet(__DIR__ . '/Fixtures/EdgeCases/interval.parquet', ['id'], engine: $engine))
                ->fetch()
                ->toArray(),
        );
    }

    #[DataProvider('reading_engines')]
    public function test_an_unsigned_integer_reads_as_itself(?ParquetEngine $engine): void
    {
        static::assertSame(
            [4_000_000_000, 1],
            array_column(
                data_frame()
                    ->read(from_parquet(__DIR__ . '/Fixtures/EdgeCases/unsigned.parquet', ['u32'], engine: $engine))
                    ->fetch()
                    ->toArray(),
                'u32',
            ),
        );
    }

    #[DataProvider('reading_engines')]
    public function test_an_enum_column_reads_as_strings(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/enum.parquet');
        Writer::php()->writeStream($memory->writeTo($path), ParquetSchema::with(FlatColumn::enum('color')), [
            ['color' => 'RED'],
            ['color' => null],
            ['color' => 'GREEN'],
        ]);

        static::assertSame(
            [['color' => 'RED'], ['color' => null], ['color' => 'GREEN']],
            data_frame()
                ->read(from_parquet($path, engine: $engine, filesystem: $memory))
                ->fetch()
                ->toArray(),
        );
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

    public function test_a_close_that_fails_during_closure_leaves_no_file(): void
    {
        $memory = memory_filesystem();
        $telemetry = new MemoryTelemetryContext();

        try {
            data_frame($telemetry->config)
                ->read(from_array([['p' => 'a', 't' => 'x'], ['p' => 'b', 't' => 'y'], ['p' => 'c', 't' => 'z']]))
                ->write(to_parquet(
                    path('memory://var/staged/file.parquet'),
                    filesystem: new FailingCloseFilesystem($memory, failingStreams: 2),
                )->partitionBy(partition_by('p')))
                ->run();
            static::fail('the run was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "memory://var/staged/p=a/file.parquet" failed', $failure->getMessage());
        }

        static::assertSame([], iterator_to_array($memory->list(path('memory://var/staged/**/*')), false));
        static::assertSame([], $telemetry->logs->entriesContaining('failed to discard'));
    }

    public function test_a_close_that_fails_while_discarding_a_failed_run_leaves_no_file(): void
    {
        $memory = memory_filesystem();

        LoaderEndingContext::failedRun(to_parquet(
            path('memory://var/failed/file.parquet'),
            filesystem: new FailingCloseFilesystem($memory),
        )->partitionBy(partition_by('id')));

        static::assertSame([], iterator_to_array($memory->list(path('memory://var/failed/**/*')), false));
    }

    public function test_a_path_only_partition_column_is_left_out_of_a_declared_file_schema(): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([
                ['id' => 'a', 'date' => new DateTimeImmutable('2026-09-01'), 'clicks' => 1],
                ['id' => 'b', 'date' => new DateTimeImmutable('2026-09-02'), 'clicks' => 2],
            ]))
            ->write(to_parquet(
                path('memory://var/declared/file.parquet'),
                schema: schema(str_schema('id'), datetime_schema('date'), int_schema('clicks')),
                filesystem: $memory,
            )->partitionBy(partition_by('date')))
            ->run();

        static::assertSame(
            ['id', 'clicks'],
            ParquetFilesContext::columnNames($memory, 'memory://var/declared/date=2026-09-01/file.parquet'),
        );
        static::assertEquals(
            [
                ['id' => 'a', 'clicks' => 1, 'date' => new DateTimeImmutable('2026-09-01 00:00:00 UTC')],
                ['id' => 'b', 'clicks' => 2, 'date' => new DateTimeImmutable('2026-09-02 00:00:00 UTC')],
            ],
            data_frame()
                ->read(from_parquet(path('memory://var/declared/**/*.parquet'), filesystem: $memory)->partitionTypes(
                    partition_types(date: type_datetime()),
                ))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_path_only_partition_column_is_left_out_of_an_inferred_file_schema(): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([
                ['id' => 'a', 'date' => new DateTimeImmutable('2026-09-01'), 'clicks' => 1],
                ['id' => 'b', 'date' => new DateTimeImmutable('2026-09-02'), 'clicks' => 2],
            ]))
            ->write(to_parquet(
                path('memory://var/inferred/file.parquet'),
                filesystem: $memory,
            )->partitionBy(partition_by('date')))
            ->run();

        static::assertSame(
            ['id', 'clicks'],
            ParquetFilesContext::columnNames($memory, 'memory://var/inferred/date=2026-09-01/file.parquet'),
        );
    }

    public function test_partitions_written_with_an_explicit_arrow_engine_each_get_their_own_file(): void
    {
        if (!extension_loaded('arrow')) {
            static::markTestSkipped('arrow is not loaded');
        }

        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([
                ['p' => 'a', 'v' => 1],
                ['p' => 'b', 'v' => 2],
                ['p' => 'a', 'v' => 3],
                ['p' => 'c', 'v' => 4],
            ]))
            ->write(to_parquet(
                path('memory://var/engine/file.parquet'),
                engine: new RustParquetEngine(),
                filesystem: $memory,
            )->partitionBy(partition_by('p')))
            ->run();

        static::assertSame(
            [['v' => 1], ['v' => 3]],
            ParquetFilesContext::values($memory, 'memory://var/engine/p=a/file.parquet'),
        );
        static::assertSame([['v' => 2]], ParquetFilesContext::values($memory, 'memory://var/engine/p=b/file.parquet'));
        static::assertSame([['v' => 4]], ParquetFilesContext::values($memory, 'memory://var/engine/p=c/file.parquet'));
    }

    public function test_partitions_written_with_an_explicit_php_engine_each_get_their_own_file(): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([
                ['p' => 'a', 'v' => 1],
                ['p' => 'b', 'v' => 2],
                ['p' => 'a', 'v' => 3],
                ['p' => 'c', 'v' => 4],
            ]))
            ->write(to_parquet(
                path('memory://var/engine/file.parquet'),
                engine: new PhpParquetEngine(),
                filesystem: $memory,
            )->partitionBy(partition_by('p')))
            ->run();

        static::assertSame(
            [['v' => 1], ['v' => 3]],
            ParquetFilesContext::values($memory, 'memory://var/engine/p=a/file.parquet'),
        );
        static::assertSame([['v' => 2]], ParquetFilesContext::values($memory, 'memory://var/engine/p=b/file.parquet'));
        static::assertSame([['v' => 4]], ParquetFilesContext::values($memory, 'memory://var/engine/p=c/file.parquet'));
    }

    #[DataProvider('engines')]
    public function test_a_zoned_datetime_reads_back_as_the_same_instant(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/zoned.parquet');

        data_frame()
            ->read(from_rows(array_to_rows([[
                'at' => new DateTimeImmutable('2026-09-01 12:30:00.123456 Europe/Warsaw'),
            ]], schema(datetime_schema('at', zone: 'Europe/Warsaw')))))
            ->write(to_parquet($path, filesystem: $memory, engine: $engine))
            ->run();

        static::assertEquals(
            [['at' => new DateTimeImmutable('2026-09-01 10:30:00.123456 UTC')]],
            data_frame()
                ->read(from_parquet($path, filesystem: $memory, engine: $engine))
                ->fetch()
                ->toArray(),
        );
    }

    #[DataProvider('engines')]
    public function test_every_partition_is_written_cast_to_the_declared_schema(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([['group' => 'a', 'id' => 1], ['group' => 'b', 'id' => 2], ['group' => 'a', 'id' => 3]]))
            ->write(to_parquet(
                path('memory://var/partitioned_cast/file.parquet'),
                schema: schema(str_schema('group'), str_schema('id')),
                filesystem: $memory,
                engine: $engine,
            )->partitionBy(partition_by('group')))
            ->run();

        static::assertSame([[['id' => '1'], ['id' => '3']], [['id' => '2']]], [
            ParquetFilesContext::phpEngineValues($memory, 'memory://var/partitioned_cast/group=a/file.parquet'),
            ParquetFilesContext::phpEngineValues($memory, 'memory://var/partitioned_cast/group=b/file.parquet'),
        ]);
    }

    #[DataProvider('engines')]
    public function test_write_columns_keeps_the_partition_column_in_the_file(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array([
                ['id' => 'a', 'date' => new DateTimeImmutable('2026-09-01'), 'clicks' => 1],
                ['id' => 'b', 'date' => new DateTimeImmutable('2026-09-02'), 'clicks' => 2],
            ]))
            ->write(
                to_parquet(
                    path('memory://var/write_columns/file.parquet'),
                    schema: schema(str_schema('id'), datetime_schema('date'), int_schema('clicks')),
                    filesystem: $memory,
                    engine: $engine,
                )->partitionBy(partition_by('date')->writeColumns()),
            )
            ->run();

        static::assertSame(
            ['id', 'date', 'clicks'],
            ParquetFilesContext::columnNames($memory, 'memory://var/write_columns/date=2026-09-01/file.parquet'),
        );
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_a_datetime_partition_reads_back_the_written_time(bool $writeColumns): void
    {
        $memory = memory_filesystem();
        $by = partition_by('date');

        data_frame()
            ->read(from_array(
                [
                    ['id' => 'a', 'date' => new DateTimeImmutable('2026-10-02 09:00:00 UTC')],
                    ['id' => 'b', 'date' => new DateTimeImmutable('2026-10-02 13:00:00.123456 UTC')],
                ],
                schema(str_schema('id'), datetime_schema('date')),
            ))
            ->write(to_parquet(path('memory://var/times/file.parquet'), filesystem: $memory)->partitionBy(
                $writeColumns ? $by->writeColumns() : $by,
            ))
            ->run();

        static::assertEquals(
            [
                ['id' => 'a', 'date' => new DateTimeImmutable('2026-10-02 09:00:00 UTC')],
                ['id' => 'b', 'date' => new DateTimeImmutable('2026-10-02 13:00:00.123456 UTC')],
            ],
            data_frame()
                ->read(from_parquet(path('memory://var/times/**/*.parquet'), filesystem: $memory)->partitionTypes(
                    partition_types(date: type_datetime()),
                ))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_float_partition_reads_back_the_written_value(): void
    {
        $memory = memory_filesystem();

        data_frame()
            ->read(from_array(
                [['id' => 'a', 'x' => 0.0], ['id' => 'b', 'x' => 1.0E-9]],
                schema(str_schema('id'), float_schema('x')),
            ))
            ->write(to_parquet(path('memory://var/floats/file.parquet'), filesystem: $memory)->partitionBy(partition_by(
                'x',
            )))
            ->run();

        static::assertSame(
            [['id' => 'a', 'x' => 0.0], ['id' => 'b', 'x' => 1.0E-9]],
            data_frame()
                ->read(from_parquet(path('memory://var/floats/**/*.parquet'), filesystem: $memory)->partitionTypes(
                    partition_types(x: type_float()),
                ))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_xml_nodes_written_before_utf8_node_documents_read_back_and_load_as_their_text(): void
    {
        $memory = memory_filesystem();
        $loaded = path('memory://var/xml-node-utf8.parquet');

        data_frame()
            ->read(from_parquet(__DIR__ . '/Fixtures/xml-node-before-utf8.parquet'))
            ->write(to_parquet($loaded, filesystem: $memory))
            ->run();

        static::assertSame(
            [
                ['node' => '<row><a>zażółć ☃ 😀</a></row>'],
                ['node' => '<row a="żółć"/>'],
                ['node' => '<row><b>&lt;p&gt; &amp; ©</b></row>'],
            ],
            data_frame()
                ->read(from_parquet($loaded, filesystem: $memory))
                ->withEntry('node', ref('node')->cast(type_string()))
                ->fetch()
                ->toArray(),
        );
    }
}
