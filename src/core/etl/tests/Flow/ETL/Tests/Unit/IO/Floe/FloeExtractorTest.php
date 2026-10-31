<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Floe\FloeExtractor;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Context\SplitCoverage;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use PHPUnit\Framework\Attributes\TestWith;

use function array_filter;
use function array_sum;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_integer;
use function iterator_to_array;
use function max;

final class FloeExtractorTest extends FlowTestCase
{
    public function test_a_later_file_with_other_columns_throws_naming_both_files(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeFiles($memory, [
            'memory://glob/a.floe' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://glob/b.floe' => array_to_rows(
                [[
                    'id' => 2,
                    'extra' => 'x',
                ]],
                schema(int_schema('id'), str_schema('extra')),
            ),
        ]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            "Columns of memory://glob/b.floe do not match the schema read from memory://glob/a.floe:\n"
            . "  Unexpected Definitions: \n"
            . '    |-- extra<string>',
        );

        iterator_to_array(FlowTestCase::extracted(
            from_floe(path('memory://glob/*.floe'), filesystem: $memory),
            flow_context(config())->backend(),
            new Criteria(),
        ));
    }

    public function test_a_later_file_with_another_column_type_throws(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeFiles($memory, [
            'memory://glob/a.floe' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://glob/b.floe' => array_to_rows([['id' => 'x']], schema(str_schema('id'))),
        ]);

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('|-- expected: id<integer>, given: id<string>');

        iterator_to_array(FlowTestCase::extracted(
            from_floe(path('memory://glob/*.floe'), filesystem: $memory),
            flow_context(config())->backend(),
            new Criteria(),
        ));
    }

    public function test_a_later_file_with_the_columns_in_another_order_follows_the_first(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeFiles($memory, [
            'memory://glob/a.floe' => array_to_rows(
                [[
                    'id' => 1,
                    'name' => 'a',
                ]],
                schema(int_schema('id'), str_schema('name')),
            ),
            'memory://glob/b.floe' => array_to_rows(
                [[
                    'name' => 'b',
                    'id' => 2,
                ]],
                schema(str_schema('name'), int_schema('id')),
            ),
        ]);

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']],
            df()
                ->read(from_floe(path('memory://glob/*.floe'), filesystem: $memory))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_union_by_name_reads_every_file_under_one_schema(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeFiles($memory, [
            'memory://glob/a.floe' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://glob/b.floe' => array_to_rows(
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
                ->read(from_floe(path('memory://glob/*.floe'), filesystem: $memory)->unionByName())
                ->fetch()
                ->toArray(),
        );
    }

    public function test_extract_adds_input_file_uri_when_configured(): void
    {
        $context = flow_context(config_builder()->build());
        $memory = memory_filesystem();
        $path = path('memory://input-uri.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $batches = iterator_to_array(FlowTestCase::extracted(
            from_floe($path, filesystem: $memory)->withMetadataColumns(true),
            $context->backend(),
            new Criteria(),
        ));

        static::assertSame($path->uri(), $batches[0]->column('_input_file_uri')->value(0));
    }

    public function test_extract_applies_offset_within_a_file(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://offset-within.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $ids = [];

        foreach (FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            $context->backend(),
            new Criteria(offset: 1),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([2, 3], $ids);
    }

    public function test_extract_honors_limit(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://limited.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $extractor = from_floe($path, filesystem: $memory);

        $ids = [];

        foreach (FlowTestCase::extracted($extractor, $context->backend(), new Criteria(limit: 2)) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_extract_omits_input_file_uri_by_default(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://no-input-uri.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        static::assertSame(
            ['id'],
            array_keys(iterator_to_array(FlowTestCase::extracted(
                from_floe($path, filesystem: $memory),
                $context->backend(),
                new Criteria(),
            ))[0]->values(0)),
        );
    }

    public function test_extract_reads_rows(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://read.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $ids = [];

        foreach (FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            $context->backend(),
            new Criteria(),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_extract_skips_whole_files_with_offset(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();

        foreach (['a' => [1, 2], 'b' => [10, 11]] as $name => $values) {
            $loader = to_floe(path('memory://skip-files/' . $name . '.floe'), filesystem: $memory);
            $loader->load(array_to_rows([
                ['id' => $values[0]],
                ['id' => $values[1]],
            ], schema(int_schema('id'))), $context->backend());
            $loader->closure();
        }

        $ids = [];

        foreach (FlowTestCase::extracted(
            from_floe(path('memory://skip-files/*.floe'), filesystem: $memory),
            $context->backend(),
            new Criteria(offset: 2),
        ) as $batch) {
            foreach ($batch->toArray() as $extractedRow) {
                $ids[] = $extractedRow['id'];
            }
        }

        static::assertSame([10, 11], $ids);
    }

    public function test_extract_stops_on_stop_signal(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://stop.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $generator = FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            $context->backend(),
            new Criteria(),
        );
        $batches = 0;

        foreach ($generator as $_batch) {
            $batches++;
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $batches);
    }

    public function test_extract_yields_the_metadata_column_schema_promises(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://declared-with-metadata.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $extractor = from_floe($path, filesystem: $memory)->withMetadataColumns(true);

        $batches = iterator_to_array(FlowTestCase::extracted($extractor, $context->backend(), new Criteria()));

        static::assertSame($extractor->schema()->names(), array_keys($batches[0]->values(0)));
        static::assertSame($path->uri(), $batches[0]->column('_input_file_uri')->value(0));
    }

    public function test_a_declared_schema_answers_without_opening_a_file(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_floe(path('memory://no-match/*.floe'), filesystem: memory_filesystem())
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://backend.floe');
        $context = flow_context(config());

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        $backend = new SpyBackend();

        iterator_to_array(FlowTestCase::extracted(
            from_floe($path, filesystem: $memory),
            flow_context(config_builder()->backend($backend)->build())->backend(),
            new Criteria(),
        ));

        static::assertGreaterThanOrEqual(1, $backend->decodeFrames());
    }

    public function test_schema_reads_footer_only(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://schema.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context->backend());
        $loader->closure();

        static::assertNotNull(from_floe($path, filesystem: $memory)->schema()->findDefinition('id'));
    }

    public function test_schema_declares_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        static::assertSame(
            ['id', 'country'],
            from_floe(path('memory://parts/*/*.floe'), filesystem: $memory)->schema()->names(),
        );
    }

    public function test_extract_fills_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $memory);
        $values = [];

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            foreach ($rows->toArray() as $row) {
                $values[] = $row;
            }
        }

        static::assertSame([['id' => 2, 'country' => 'DE'], ['id' => 1, 'country' => 'PL']], $values);
    }

    public function test_schema_opens_only_the_first_file(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->schema();

        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_schema_is_memoised(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $counting);

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_union_by_name_opens_every_file(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->unionByName()->schema();

        static::assertSame(2, $counting->readFromCalls);
    }

    public function test_schema_closes_every_reader_it_opens(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->unionByName()->schema();

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_declared_partition_types_reach_the_rows(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeYearPartitionedFiles($memory);

        $extractor = from_floe(path('memory://years/*/*.floe'), filesystem: $memory)->partitionTypes(
            partition_types(year: type_integer()),
        );

        foreach (FlowTestCase::extracted($extractor, flow_context(config())->backend(), new Criteria()) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertSame(2024, $rows->column('year')->value(0));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_extract_closes_the_reader_when_the_generator_is_abandoned(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        $generator = FlowTestCase::extracted(
            from_floe(path('memory://parts/*/*.floe'), filesystem: $counting),
            flow_context(config())->backend(),
            new Criteria(),
        );
        $generator->current();
        unset($generator);

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_every_reader_it_opens(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        foreach (FlowTestCase::extracted(
            from_floe(path('memory://parts/*/*.floe'), filesystem: $counting),
            flow_context(config())->backend(),
            new Criteria(),
        ) as $_rows) {
        }

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_the_reader_it_skips_for_the_offset(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        foreach (FlowTestCase::extracted(
            from_floe(path('memory://parts/*/*.floe'), filesystem: $counting),
            flow_context(config())->backend(),
            new Criteria(offset: 1),
        ) as $_rows) {
        }

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_the_reader_when_the_pipeline_stops(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        $generator = FlowTestCase::extracted(
            from_floe(path('memory://parts/*/*.floe'), filesystem: $counting),
            flow_context(config())->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_a_path_filter_narrows_the_read_but_not_the_schema(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $counting);
        $schema = $extractor->schema();

        static::assertSame(
            [],
            iterator_to_array(FlowTestCase::extracted(
                $extractor,
                flow_context(config())->backend(),
                new Criteria(pathFilter: new RejectingFilter()),
            )),
        );
        static::assertSame(1, $counting->readFromCalls);
        static::assertEquals($schema, $extractor->schema());
    }

    public function test_source_returns_path(): void
    {
        static::assertSame(
            'memory://x.floe',
            from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->source()->uri(),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->isRepeatable());
    }

    public function test_floe_extractor_honours_the_batch_contract(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://contract.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(RowsMother::sequentialIds(7), $context->backend());
        $loader->closure();

        self::assertExtractorHonoursBatchContract(
            static fn(): FloeExtractor => from_floe($path, filesystem: $memory),
            RowsMother::sequentialIds(7),
        );
    }

    public function test_reader_batch_follows_the_batch_size(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://twenty.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(RowsMother::sequentialIds(20), $context->backend());
        $loader->closure();

        $sizes = [];

        foreach (FlowTestCase::extracted(
            from_floe($path, filesystem: $memory)->withBatchSize(7),
            $context->backend(),
            new Criteria(),
        ) as $rows) {
            $sizes[] = $rows->count();
        }

        static::assertLessThanOrEqual(7, max($sizes));
        static::assertSame(20, array_sum($sizes));
    }

    public function test_an_empty_glob_is_one_split_reading_nothing(): void
    {
        static::assertSame(
            [0],
            SplitCoverage::rowCountPerSplit(
                from_floe(path('memory://split/*.floe'), filesystem: memory_filesystem())->withSchema(schema(int_schema(
                    'id',
                ))),
                flow_context(),
                1,
            ),
        );
    }

    public function test_each_section_is_owned_by_the_window_holding_its_midpoint(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);

        static::assertSame(
            [100, 100, 100],
            array_values(array_filter(SplitCoverage::rowCountPerSplit(
                from_floe(path('memory://split/a.floe'), filesystem: $memory),
                flow_context(),
                100,
            ))),
        );
    }

    public function test_extract_split_reads_no_more_than_the_limit(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);
        $extractor = from_floe(path('memory://split/a.floe'), filesystem: $memory);
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

    public function test_extract_split_stops_on_the_stop_signal(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);
        $extractor = from_floe(path('memory://split/a.floe'), filesystem: $memory);
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
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);
        $generator = FlowTestCase::extracted(
            from_floe(path('memory://split/a.floe'), filesystem: $memory)->withBatchSize(1),
            flow_context()->backend(),
            (new Criteria())->withSplit(new FileSplit(path('memory://split/a.floe'), 0, null)),
        );
        $totalRows = 0;

        foreach ($generator as $rows) {
            $totalRows += $rows->count();
            $generator->send(Signal::STOP);
        }

        static::assertSame(1, $totalRows);
    }

    /**
     * @param int<1, max> $targetSplitBytes
     */
    #[TestWith([50])]
    #[TestWith([500])]
    #[TestWith([2_000])]
    #[TestWith([8 * 1024 * 1024])]
    public function test_splits_cover_every_row_exactly_once(int $targetSplitBytes): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);
        FloeFilesContext::writeSections($memory, 'memory://split/b.floe', 2, 100);

        static::assertSame(
            500,
            array_sum(SplitCoverage::rowCountPerSplit(
                from_floe(path('memory://split/*.floe'), filesystem: $memory),
                flow_context(),
                $targetSplitBytes,
            )),
        );
    }

    public function test_splits_preserve_source_order(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 3, 100);
        FloeFilesContext::writeSections($memory, 'memory://split/b.floe', 2, 100);
        $extractor = from_floe(path('memory://split/*.floe'), filesystem: $memory);

        static::assertEquals(
            ExtractedRows::of($extractor),
            SplitCoverage::rowsFromSplits($extractor, flow_context(), 500),
        );
    }

    public function test_a_pushed_path_filter_prunes_the_files_splits_are_cut_from(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writeSections($memory, 'memory://split/a.floe', 1, 10);
        $extractor = from_floe(path('memory://split/*.floe'), filesystem: $memory);

        static::assertEquals(
            [new FileSplit($extractor->source(), 0, null)],
            $extractor->splits((new Criteria())->withPathFilter(new RejectingFilter()), 1),
        );
    }
}
