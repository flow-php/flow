<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateTimeImmutable;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Plan\Stage;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
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
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
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

final class ParquetExtractorTest extends FlowTestCase
{
    public function test_limit(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config()), limit: 2) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(2, $extractedRows);
    }

    public function test_reading_file_from_given_offset(): void
    {
        $totalRows = (new Reader())
            ->read(__DIR__ . '/Fixtures/orders_1k.parquet')
            ->metadata()
            ->rowsNumber();

        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/orders_1k.parquet'))->withOffset($totalRows - 100);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(100, $extractedRows);
    }

    public function test_calling_schema_does_not_change_extraction(): void
    {
        $cold = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        $warm = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));
        $warm->schema();
        $warm->withMetadataColumns(true);

        static::assertEquals(
            iterator_to_array($cold->extract(flow_context(config()))),
            iterator_to_array($warm->extract(flow_context(config()))),
        );
    }

    public function test_extract_yields_the_metadata_column_schema_promises(): void
    {
        $extractor = from_parquet(
            path(__DIR__ . '/Fixtures/orders_1k.parquet'),
            columns: ['email'],
        )->withMetadataColumns(true);

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            static::assertSame($extractor->schema()->references()->names(), $batch->schema()->references()->names());

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
                from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
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
            array_keys(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->schema()->definitions()),
        );
    }

    public function test_schema_is_idempotent(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        static::assertEquals($extractor->schema(), $extractor->schema());
    }

    public function test_schema_is_narrowed_down_to_selected_columns(): void
    {
        static::assertSame(
            ['order_id', 'email'],
            array_keys(
                from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_schema_declares_partition_columns_from_the_path(): void
    {
        static::assertSame(
            ['id', 'name', 'date'],
            from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'))
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_extract_fills_partition_columns_from_the_path(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'));

        foreach ($extractor->extract(flow_context(config())) as $rows) {
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

        from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)->schema();

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_schema_is_memoised(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());
        $extractor = from_parquet(
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_union_by_name_opens_every_file(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)
            ->unionByName()
            ->schema();

        static::assertSame(5, $filesystem->readFromCalls);
    }

    public function test_schema_closes_every_file_it_opens(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)
            ->unionByName()
            ->schema();

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_declared_partition_types_reach_the_rows(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/date=2024-01-01/*.parquet'))
            ->partitionTypes(partition_types(date: type_datetime()));

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:00 UTC'), $rows->column('date')->value(0));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_extract_closes_the_reader_when_the_generator_is_abandoned(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $generator = from_parquet(
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        )->extract(flow_context(config()));
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
                ->read(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet')))
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
            ->read(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), filesystem: $filesystem))
            ->count();

        static::assertSame(
            df()
                ->read(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet')))
                ->fetch()
                ->count(),
            $counted,
        );
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_extract_closes_every_reader_it_opens(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        foreach (from_parquet(
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        )->extract(flow_context(config())) as $_rows) {
        }

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_extract_closes_the_reader_it_skips_for_the_offset(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        foreach (from_parquet(path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'), filesystem: $filesystem)
            ->withOffset(2500)
            ->extract(flow_context(config())) as $_rows) {
        }

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_extract_closes_the_reader_when_the_pipeline_stops(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $generator = from_parquet(
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        )->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_schema_forgets_the_fold_when_the_byte_order_changes(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        $extractor = from_parquet(
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
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
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
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
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
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
            path(__DIR__ . '/Fixtures/Pagination/partitioned/*/*.parquet'),
            filesystem: $filesystem,
        );
        $schema = $extractor->schema();

        $batches = iterator_to_array($extractor->extract(flow_context(config()), pathFilter: new RejectingFilter()));

        static::assertSame([], $batches);
        static::assertSame(1, $filesystem->readFromCalls);
        static::assertEquals($schema, $extractor->schema());
    }

    public function test_signal_stop(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_signal_stop_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $generator = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'))
            ->withBatchSize(1500)
            ->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_limit_reached_on_the_first_file_tail_batch_skips_the_remaining_files(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withBatchSize(1500);

        static::assertCount(1000, ExtractedRows::of($extractor, limit: 1000));
    }

    public function test_the_second_file_is_asked_for_the_remainder_of_the_limit(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withBatchSize(1000);

        // 01_1000.parquet fills the first batch; 02_500.parquet must be read up to the 200 rows still
        // wanted, not up to the full limit again
        $batches = iterator_to_array($extractor->extract(flow_context(config()), limit: 1200), false);

        static::assertCount(2, $batches);
        static::assertSame(1200, $batches[0]->count() + $batches[1]->count());
    }

    public function test_limit_reached_on_a_full_batch_of_the_first_file_skips_the_remaining_files(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withBatchSize(500);

        static::assertCount(1000, ExtractedRows::of($extractor, limit: 1000));
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->isRepeatable());
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
            path_real(__DIR__ . '/Fixtures/Pagination/*.parquet'),
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
        $name = $definition->entry()->name();

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
}
