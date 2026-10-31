<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration\IO\Extractor;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\ReadPlanRecordingParquetEngine;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Planning\Logical\Stage;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\SplitCoverage;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema\Definition;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_filter;
use function array_keys;
use function array_map;
use function array_sum;
use function array_unique;
use function array_values;
use function count;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function iterator_to_array;

final class ParquetExtractorTest extends FlowIntegrationTestCase
{
    /**
     * @return Generator<string, array{?ParquetEngine}>
     */
    public static function engines(): Generator
    {
        yield 'default engine' => [null];
        yield 'php engine' => [new PhpParquetEngine()];
    }

    public function test_a_declared_schema_reads_files_by_name(): void
    {
        df()
            ->read(from_array([['id' => 1, 'name' => 'one']]))
            ->write(to_parquet($this->cacheDir->suffix('by-name.parquet')))
            ->run();

        static::assertSame(
            [['id' => 1]],
            df()
                ->read(from_parquet(
                    $this->cacheDir->suffix('by-name.parquet'),
                    filesystem: $this->fs(),
                )->withSchema(schema(int_schema('id'))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_declared_nullable_column_a_file_lacks_reads_as_null(): void
    {
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_parquet($this->cacheDir->suffix('nullable.parquet')))
            ->run();

        static::assertSame(
            [['id' => 1, 'name' => null]],
            df()
                ->read(from_parquet(
                    $this->cacheDir->suffix('nullable.parquet'),
                    filesystem: $this->fs(),
                )->withSchema(schema(int_schema('id'), str_schema('name', nullable: true))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_declared_non_nullable_column_a_file_lacks_fails(): void
    {
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_parquet($this->cacheDir->suffix('not-null.parquet')))
            ->run();

        $this->expectException(SchemaMismatchException::class);

        df()
            ->read(from_parquet(
                $this->cacheDir->suffix('not-null.parquet'),
                filesystem: $this->fs(),
            )->withSchema(schema(int_schema('id'), str_schema('name'))))
            ->fetch();
    }

    public function test_a_declared_schema_skips_the_divergence_check(): void
    {
        df()
            ->read(from_array([['id' => 1, 'a' => 'x']]))
            ->write(to_parquet($this->cacheDir->suffix('diverging/a.parquet')))
            ->run();
        df()
            ->read(from_array([['id' => 2, 'b' => 'y']]))
            ->write(to_parquet($this->cacheDir->suffix('diverging/b.parquet')))
            ->run();

        static::assertSame(
            [['id' => 1, 'a' => 'x', 'b' => null], ['id' => 2, 'a' => null, 'b' => 'y']],
            df()
                ->read(from_parquet(
                    $this->cacheDir->suffix('diverging/*.parquet'),
                    filesystem: $this->fs(),
                )->withSchema(schema(
                    int_schema('id', nullable: true),
                    str_schema('a', nullable: true),
                    str_schema('b', nullable: true),
                )))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_with_columns_narrows_a_declared_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_parquet($this->cacheDir->suffix('narrowed/*.parquet'), filesystem: $this->fs())
                ->withSchema(schema(int_schema('id'), str_schema('name')))
                ->withColumns(['id'])
                ->schema(),
        );
    }

    public function test_a_serial_read_lists_the_source_once(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        static::assertCount(
            1000,
            df()
                ->read(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'), filesystem: $filesystem))
                ->fetch(),
        );
        static::assertSame(1, $filesystem->listCalls);
    }

    public function test_a_read_whose_path_filter_rejects_every_file_closes_the_file_the_schema_kept_open(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'), filesystem: $filesystem);
        $extractor->schema();

        iterator_to_array(
            FlowTestCase::extracted(
                $extractor,
                flow_context(config())->backend(),
                new Criteria(pathFilter: new RejectingFilter()),
            ),
            false,
        );

        static::assertCount(1, $filesystem->openedStreams);
        static::assertSame(1, $filesystem->closedStreams());
    }

    public function test_a_glob_without_files_refuses_to_infer(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_parquet() has nothing to infer a schema from: no file matching "'
            . $this->cacheDir->suffix('**/*.parquet')->uri()
            . '" carries a column. Declare the schema with ->withSchema(...).',
        );

        df()
            ->read(from_parquet($this->cacheDir->suffix('**/*.parquet'), filesystem: $this->fs()))
            ->select('id')
            ->fetch();
    }

    public function test_a_glob_without_files_reads_nothing_under_a_declared_schema(): void
    {
        static::assertCount(
            0,
            df()
                ->read(from_parquet(
                    $this->cacheDir->suffix('**/*.parquet'),
                    filesystem: $this->fs(),
                )->withSchema(schema(int_schema('id'))))
                ->select('id')
                ->fetch(),
        );
    }

    #[DataProvider('engines')]
    public function test_a_body_predicate_skips_row_groups(?ParquetEngine $engine): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://groups.parquet', 3000, 1000);
        $filtered = new CountingFilesystem($memory);
        $unfiltered = new CountingFilesystem($memory);
        $predicate = ref('id')->greaterThanEqual(lit(2500));

        $rows = df()
            ->read(from_parquet(path('memory://groups.parquet'), filesystem: $filtered, engine: $engine))
            ->filter($predicate)
            ->fetch();
        $all = df()
            ->read(from_parquet(path('memory://groups.parquet'), filesystem: $unfiltered, engine: $engine))
            ->fetch();

        static::assertCount(500, $rows);
        static::assertSame(df()->read(from_rows($all))->filter($predicate)->fetch()->toArray(), $rows->toArray());
        static::assertLessThan(ParquetFilesContext::bytesRead($unfiltered), ParquetFilesContext::bytesRead($filtered));
    }

    public function test_limit(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'));

        $extractedRows = 0;

        foreach (FlowTestCase::extracted(
            $extractor,
            flow_context(config())->backend(),
            new Criteria(limit: 2),
        ) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(2, $extractedRows);
    }

    public function test_calling_schema_does_not_change_extraction(): void
    {
        $cold = from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        $warm = from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'));
        $warm->schema();
        $warm->withMetadataColumns(true);

        static::assertEquals(
            iterator_to_array(FlowTestCase::extracted($cold, flow_context(config())->backend(), new Criteria())),
            iterator_to_array(FlowTestCase::extracted($warm, flow_context(config())->backend(), new Criteria())),
        );
    }

    public function test_extract_yields_the_metadata_column_schema_promises(): void
    {
        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'),
            columns: ['email'],
        )->withMetadataColumns(true);

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $batch) {
            static::assertSame($extractor->schema()->names(), $batch->schema()->names());

            break;
        }
    }

    public function test_nested_uuid_and_json_read_as_value_objects(): void
    {
        $memory = memory_filesystem();
        $uuid = '5f0e3a0e-0b8a-4b7a-9d6e-1f2a3b4c5d6e';

        df()
            ->read(from_rows(array_to_rows(
                [['lu' => [$uuid], 's' => ['j' => ['b' => 2], 'u' => $uuid], 'm' => ['k' => $uuid]]],
                schema(
                    list_schema('lu', type_list(type_uuid())),
                    structure_schema('s', type_structure(['j' => type_json(), 'u' => type_uuid()])),
                    map_schema('m', type_map(type_string(), type_uuid())),
                ),
            )))
            ->write(to_parquet(path('memory://nested.parquet'), filesystem: $memory))
            ->run();

        $rows = df()->read(from_parquet(path('memory://nested.parquet'), filesystem: $memory))->fetch();

        // @mago-ignore analysis:mixed-assignment
        $list = $rows->column('lu')->value(0);
        // @mago-ignore analysis:mixed-assignment
        $structure = $rows->column('s')->value(0);
        // @mago-ignore analysis:mixed-assignment
        $map = $rows->column('m')->value(0);

        static::assertIsArray($list);
        static::assertInstanceOf(Uuid::class, $list[0]);
        static::assertIsArray($structure);
        static::assertInstanceOf(Json::class, $structure['j']);
        static::assertInstanceOf(Uuid::class, $structure['u']);
        static::assertIsArray($map);
        static::assertInstanceOf(Uuid::class, $map['k']);
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertSame(
            ['order_id', 'email', '_input_file_uri'],
            array_keys(
                from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
                    ->withMetadataColumns(true)
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_schema_comes_from_the_file_footer(): void
    {
        static::assertSame(
            ['order_id', 'created_at', 'updated_at', 'discount', 'email', 'customer', 'address', 'notes', 'items'],
            array_keys(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'))->schema()->definitions()),
        );
    }

    public function test_schema_is_idempotent(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        static::assertEquals($extractor->schema(), $extractor->schema());
    }

    public function test_schema_is_narrowed_down_to_selected_columns(): void
    {
        static::assertSame(
            ['order_id', 'email'],
            array_keys(
                from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_schema_declares_partition_columns_from_the_path(): void
    {
        static::assertSame(
            ['id', 'name', 'date'],
            from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'))
                ->schema()
                ->names(),
        );
    }

    public function test_extract_fills_partition_columns_from_the_path(): void
    {
        $extractor = from_parquet(path(__DIR__
        . '/../../../Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'));

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            static::assertSame(['id', 'name', 'date'], array_keys($rows->toArray()[0]));
            static::assertSame('2024-01-01', $rows->column('date')->value(0));
            static::assertEquals($extractor->schema(), $rows->schema());

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_schema_opens_only_the_first_file(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        )->schema();

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_schema_is_memoised(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_union_by_name_opens_every_file(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)
            ->unionByName()
            ->schema();

        static::assertSame(5, $filesystem->readFromCalls);
    }

    public function test_schema_closes_every_file_it_opens(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)
            ->unionByName()
            ->schema();

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_declared_partition_types_reach_the_rows(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'))
            ->partitionTypes(partition_types(date: type_datetime()));

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:00 UTC'), $rows->column('date')->value(0));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_extract_closes_the_reader_when_the_generator_is_abandoned(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $generator = FlowTestCase::extracted(
            from_parquet(
                path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
                filesystem: $filesystem,
            ),
            flow_context(config())->backend(),
            new Criteria(),
        );
        $generator->current();
        unset($generator);

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_explain_lists_the_statistics_the_file_declares(): void
    {
        static::assertStringContainsString(
            <<<'PLAN'
                      Extractor: ParquetExtractor
                         Statistics: rows exact 1 000 · size exact 327 527 B
                PLAN,
            df()
                ->read(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet')))
                ->explain()
                ->toString(Stage::physical),
        );
    }

    public function test_analyze_reports_the_extrapolated_rows_next_to_the_rows_read(): void
    {
        $memory = memory_filesystem();
        df()
            ->read(from_sequence_number('id', 1, 10_000))
            ->write(to_parquet(path('memory://glob/a.parquet'), filesystem: $memory))
            ->run();
        df()
            ->read(from_sequence_number('id', 1, 19_000))
            ->write(to_parquet(path('memory://glob/b.parquet'), filesystem: $memory))
            ->run();

        $report = df()->read(from_parquet(path('memory://glob/*.parquet'), filesystem: $memory))->run(
            analyze()->withSourceStatistics(),
        );

        static::assertNotNull($report);
        $sources = $report->sources();
        static::assertNotNull($sources);
        static::assertCount(1, $sources);
        static::assertSame('ParquetExtractor', $sources[0]->extractor);
        static::assertEquals(Cardinality::approximately(20_000), $sources[0]->declared->rows);
        static::assertSame(29_000, $sources[0]->rows);
        static::assertEqualsWithDelta(9_000 / 29_000, $sources[0]->rowsError(), 0.000_001);
    }

    public function test_count_answers_from_the_footer_without_reading_the_rows(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $counted = df()
            ->read(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'), filesystem: $filesystem))
            ->count();

        static::assertSame(
            df()
                ->read(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet')))
                ->fetch()
                ->count(),
            $counted,
        );
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_extract_closes_every_reader_it_opens(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        foreach (FlowTestCase::extracted(
            from_parquet(
                path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
                filesystem: $filesystem,
            ),
            flow_context(config())->backend(),
            new Criteria(),
        ) as $_rows) {
        }

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_extract_closes_the_reader_it_skips_for_the_offset(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        foreach (FlowTestCase::extracted(
            from_parquet(
                path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
                filesystem: $filesystem,
            ),
            flow_context(config())->backend(),
            new Criteria(offset: 2500),
        ) as $_rows) {
        }

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_extract_closes_the_reader_when_the_pipeline_stops(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $generator = FlowTestCase::extracted(
            from_parquet(
                path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
                filesystem: $filesystem,
            ),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_schema_forgets_the_fold_when_the_byte_order_changes(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );
        $extractor->schema();
        // the same value: the point is that the setter clears the memo, not that the value differs
        $extractor->withByteOrder(ByteOrder::LITTLE_ENDIAN)->schema();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_schema_forgets_the_fold_when_the_engine_changes(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );
        $extractor->schema();
        $extractor->withEngine(new PhpParquetEngine())->schema();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_schema_forgets_the_fold_when_the_options_change(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );
        $extractor->schema();
        $extractor->withOptions(Options::default())->schema();

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_a_path_filter_narrows_the_read_but_not_the_schema(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $extractor = from_parquet(
            path(__DIR__ . '/../../../Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );
        $schema = $extractor->schema();

        static::assertSame(
            [],
            iterator_to_array(FlowTestCase::extracted(
                $extractor,
                flow_context(config())->backend(),
                new Criteria(pathFilter: new RejectingFilter()),
            )),
        );
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertEquals($schema, $extractor->schema());
    }

    public function test_signal_stop(): void
    {
        $generator = FlowTestCase::extracted(
            from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/*.parquet')),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_signal_stop_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $generator = FlowTestCase::extracted(
            from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/*.parquet'))->withBatchSize(1500),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_limit_reached_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        static::assertCount(1000, ExtractedRows::of(
            from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/*.parquet'))->withBatchSize(1500),
            criteria: new Criteria(1000),
        ));
    }

    public function test_the_second_file_is_asked_for_the_remainder_of_the_limit(): void
    {
        // 01_1000.parquet fills the first batch; 02_500.parquet must be read up to the 200 rows still
        // wanted, not up to the full limit again
        $batches = iterator_to_array(
            FlowTestCase::extracted(
                from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/*.parquet'))->withBatchSize(1000),
                flow_context(config())->backend(),
                new Criteria(limit: 1200),
            ),
            false,
        );

        static::assertCount(2, $batches);
        static::assertSame(1200, $batches[0]->count() + $batches[1]->count());
    }

    public function test_limit_reached_on_a_full_batch_of_the_first_file_skips_the_remaining_files(): void
    {
        static::assertCount(1000, ExtractedRows::of(
            from_parquet(path(__DIR__ . '/../../../Fixtures/Pagination/*.parquet'))->withBatchSize(500),
            criteria: new Criteria(1000),
        ));
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_parquet(path(__DIR__ . '/../../../Fixtures/orders_1k.parquet'))->isRepeatable());
    }

    public function test_a_later_file_with_other_columns_throws_naming_both_files(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::write($memory, [
            'memory://glob/a.parquet' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://glob/b.parquet' => array_to_rows(
                [[
                    'id' => 2,
                    'extra' => 'x',
                ]],
                schema(int_schema('id'), str_schema('extra')),
            ),
        ]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'Columns of memory://glob/b.parquet do not match the schema read from memory://glob/a.parquet:',
        );

        df()->read(from_parquet(path('memory://glob/*.parquet'), filesystem: $memory))->fetch();
    }

    public function test_union_by_name_reads_every_file_under_one_schema(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::write($memory, [
            'memory://glob/a.parquet' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://glob/b.parquet' => array_to_rows(
                [[
                    'id' => 2,
                    'extra' => 'x',
                ]],
                schema(int_schema('id'), str_schema('extra')),
            ),
        ]);

        static::assertSame(
            [['id' => 1, 'extra' => null], ['id' => 2, 'extra' => 'x']],
            df()
                ->read(from_parquet(path('memory://glob/*.parquet'), filesystem: $memory)->unionByName())
                ->fetch()
                ->toArray(),
        );
    }

    public function test_one_extractor_read_twice_interleaved_gives_each_read_every_row_and_closes_every_stream(): void
    {
        $filesystem = new RecordingFilesystem(native_local_filesystem());
        [$first, $second] = ExtractedRows::interleaved((new ParquetExtractor(
            path_real(__DIR__ . '/../../../Fixtures/Pagination/*.parquet'),
            $filesystem,
        ))->withBatchSize(7));

        static::assertGreaterThan(0, $first->count());
        static::assertSame($first->toArray(), $second->toArray());
        static::assertSame(
            count(array_keys($filesystem->calls, 'readFrom', true)),
            count(array_keys($filesystem->calls, 'closeSource', true)),
        );
    }

    public static function partition_columns_the_file_holds(): Generator
    {
        yield 'datetime' => [datetime_schema('date'), new DateTimeImmutable('2026-10-02 13:00:00 UTC')];
        yield 'date' => [date_schema('day'), new DateTimeImmutable('2026-10-02')];
        yield 'integer' => [int_schema('year'), 2026];
    }

    /**
     * @param Definition<mixed> $definition
     */
    #[DataProvider('partition_columns_the_file_holds')]
    public function test_partition_schema_types_a_partition_column_the_file_holds_like_the_read(
        Definition $definition,
        mixed $value,
    ): void {
        $memory = memory_filesystem();
        $name = $definition->name();

        df()
            ->read(from_array([['id' => 'a', $name => $value]], schema(str_schema('id'), $definition)))
            ->write(
                to_parquet(path('memory://var/typed/file.parquet'), filesystem: $memory)->partitionBy(
                    partition_by($name)->writeColumns(),
                ),
            )
            ->run();

        $extractor = from_parquet(path('memory://var/typed/**/*.parquet'), filesystem: $memory);

        static::assertEquals($extractor->schema()->keep($name), $extractor->partitionSchema());
    }

    public function test_partition_schema_types_a_path_only_partition_column_as_a_string(): void
    {
        $memory = memory_filesystem();

        df()
            ->read(from_array(
                [['id' => 'a', 'date' => new DateTimeImmutable('2026-10-02 13:00:00 UTC')]],
                schema(str_schema('id'), datetime_schema('date')),
            ))
            ->write(to_parquet(
                path('memory://var/path_only/file.parquet'),
                filesystem: $memory,
            )->partitionBy(partition_by('date')))
            ->run();

        static::assertEquals(
            schema(str_schema('date')),
            from_parquet(path('memory://var/path_only/**/*.parquet'), filesystem: $memory)->partitionSchema(),
        );
    }

    public function test_an_offset_skips_the_row_groups_it_covers_from_the_footer(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://offset/a.parquet', 1_000, 100);
        $engine = new ReadPlanRecordingParquetEngine();

        static::assertSame(
            [900, 901, 902],
            df()
                ->read(from_parquet('memory://offset/a.parquet', engine: $engine, filesystem: $memory))
                ->offset(900)
                ->limit(3)
                ->fetch()
                ->reduceToArray('id'),
        );
        static::assertSame([1], $engine->selectedRowGroups);
        static::assertSame(10, $engine->rowGroups);
    }

    public function test_an_offset_counts_the_rows_a_pushed_filter_keeps(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://offset/a.parquet', 1_000, 100);

        static::assertSame(
            [920, 940, 960, 980],
            df()
                ->read(from_parquet('memory://offset/a.parquet', filesystem: $memory))
                ->filter(ref('id')->mod(lit(20))->equals(lit(0)))
                ->offset(46)
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_an_offset_past_the_last_row_yields_no_rows(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::rowGroups($memory, 'memory://offset/a.parquet', 300, 100);

        static::assertSame(
            0,
            df()->read(from_parquet('memory://offset/a.parquet', filesystem: $memory))->offset(1_000)->count(),
        );
    }

    public function test_a_glob_splits_per_file_then_per_row_group(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/b.parquet', 300);

        $splits = from_parquet('memory://split/*.parquet', filesystem: $filesystem)->splits(new Criteria(), 1_000);

        static::assertSame(
            ['memory://split/a.parquet', 'memory://split/b.parquet'],
            array_values(array_unique(array_map(static fn(FileSplit $split): string => $split->path->uri(), $splits))),
        );
        static::assertGreaterThan(2, count($splits));
    }

    public function test_a_single_row_group_file_is_one_split(): void
    {
        static::assertSame(
            [1000],
            SplitCoverage::rowCountPerSplit(
                from_parquet(__DIR__ . '/../../../Fixtures/orders_1k.parquet'),
                flow_context(),
                8 * 1024 * 1024,
            ),
        );
    }

    public function test_an_empty_glob_is_one_split_reading_nothing(): void
    {
        static::assertSame(
            [0],
            SplitCoverage::rowCountPerSplit(
                from_parquet('memory://split/*.parquet', filesystem: memory_filesystem())->withSchema(schema(int_schema(
                    'id',
                ))),
                flow_context(),
                1,
            ),
        );
    }

    public function test_extract_split_stops_on_the_stop_signal(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        $extractor = from_parquet('memory://split/a.parquet', filesystem: $filesystem);
        $extractor->withBatchSize(1);
        $generator = FlowTestCase::extracted(
            $extractor,
            flow_context()->backend(),
            (new Criteria())->withSplit($extractor->splits(new Criteria(), 100_000)[0]),
        );
        $totalRows = 0;

        foreach ($generator as $rows) {
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $totalRows);
    }

    public function test_extract_split_stops_on_the_stop_signal_of_a_whole_source_split(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        $generator = FlowTestCase::extracted(
            from_parquet('memory://split/a.parquet', filesystem: $filesystem)->withBatchSize(1),
            flow_context()->backend(),
            (new Criteria())->withSplit(new FileSplit(path('memory://split/a.parquet'), 0, null)),
        );
        $totalRows = 0;

        foreach ($generator as $rows) {
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $totalRows);
    }

    public function test_extract_split_reads_no_more_than_the_limit(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        $extractor = from_parquet('memory://split/a.parquet', filesystem: $filesystem);
        $extractor->withBatchSize(10);
        $total = 0;

        foreach (FlowTestCase::extracted(
            $extractor,
            flow_context()->backend(),
            (new Criteria(25))->withSplit($extractor->splits(new Criteria(), 100_000)[0]),
        ) as $rows) {
            $total += $rows->count();
        }

        static::assertSame(25, $total);
    }

    public function test_row_groups_are_owned_by_the_window_holding_their_midpoint(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);

        static::assertSame(
            [90, 90, 90, 30],
            array_values(array_filter(SplitCoverage::rowCountPerSplit(
                from_parquet('memory://split/a.parquet', filesystem: $filesystem),
                flow_context(),
                100,
            ))),
        );
    }

    /**
     * @param int<1, max> $targetSplitBytes
     */
    #[TestWith([100])]
    #[TestWith([500])]
    #[TestWith([2_000])]
    #[TestWith([8 * 1024 * 1024])]
    public function test_splits_cover_every_row_exactly_once(int $targetSplitBytes): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/b.parquet', 200);

        static::assertSame(
            500,
            array_sum(SplitCoverage::rowCountPerSplit(
                from_parquet('memory://split/*.parquet', filesystem: $filesystem),
                flow_context(),
                $targetSplitBytes,
            )),
        );
    }

    public function test_splits_preserve_source_order(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 300);
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/b.parquet', 200);
        $extractor = from_parquet('memory://split/*.parquet', filesystem: $filesystem);

        static::assertEquals(
            ExtractedRows::of($extractor),
            SplitCoverage::rowsFromSplits($extractor, flow_context(), 500),
        );
    }

    public function test_a_pushed_path_filter_prunes_the_files_splits_are_cut_from(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::writeRowGroups($filesystem, 'memory://split/a.parquet', 10);
        $extractor = from_parquet('memory://split/*.parquet', filesystem: $filesystem);

        static::assertEquals(
            [new FileSplit($extractor->source(), 0, null)],
            $extractor->splits((new Criteria())->withPathFilter(new RejectingFilter()), 1),
        );
    }

    public function test_a_source_read_under_a_pushed_filter_reports_no_rows_error(): void
    {
        $memory = memory_filesystem();
        df()
            ->read(from_sequence_number('id', 1, 3000))
            ->write(to_parquet(
                path('memory://analyze/ids.parquet'),
                Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
                    Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
                    1000,
                ),
                filesystem: $memory,
            ))
            ->run();

        $report = df()
            ->read(from_parquet(path('memory://analyze/ids.parquet'), filesystem: $memory))
            ->filter(ref('id')->greaterThan(lit(2500)))
            ->run(analyze: analyze()->withSourceStatistics());

        static::assertNotNull($report);
        $sources = $report->sources();
        static::assertNotNull($sources);
        static::assertCount(1, $sources);
        static::assertFalse($sources[0]->complete);
        static::assertLessThan(3000, $sources[0]->rows);
        static::assertNull($sources[0]->rowsError());
    }
}
