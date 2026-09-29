<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Flow\Floe\FloeExtractor;
use Flow\Floe\Tests\Context\FloeFilesContext;

use function array_sum;
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

        iterator_to_array(from_floe(path('memory://glob/*.floe'), filesystem: $memory)->extract(
            flow_context(config()),
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

        iterator_to_array(from_floe(path('memory://glob/*.floe'), filesystem: $memory)->extract(
            flow_context(config()),
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
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory)->withMetadataColumns(true);
        $batches = iterator_to_array($extractor->extract($context));

        static::assertSame($path->uri(), $batches[0]->column('_input_file_uri')->value(0));
    }

    public function test_extract_applies_offset_within_a_file(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://offset-within.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path, filesystem: $memory)->withOffset(1)->extract($context) as $batch) {
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
        $loader->load(array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory);

        $ids = [];

        foreach ($extractor->extract($context, limit: 2) as $batch) {
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
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $batches = iterator_to_array(from_floe($path, filesystem: $memory)->extract($context));

        static::assertSame(['id'], array_keys($batches[0]->values(0)));
    }

    public function test_extract_reads_rows(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://read.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path, filesystem: $memory)->extract($context) as $batch) {
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
            ], schema(int_schema('id'))), $context);
            $loader->closure($context);
        }

        $ids = [];

        foreach (from_floe(path('memory://skip-files/*.floe'), filesystem: $memory)
            ->withOffset(2)
            ->extract($context) as $batch) {
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
        $loader->load(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $generator = from_floe($path, filesystem: $memory)->extract($context);
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
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory)->withMetadataColumns(true);

        $batches = iterator_to_array($extractor->extract($context));

        static::assertSame($extractor->schema()->references()->names(), array_keys($batches[0]->values(0)));
        static::assertSame($path->uri(), $batches[0]->column('_input_file_uri')->value(0));
    }

    public function test_with_schema_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Floe is a self-describing format and does not accept a schema; declaring one is not supported yet.',
        );

        from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->withSchema(schema(int_schema('id')));
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://backend.floe');
        $context = flow_context(config());

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $backend = new SpyBackend();

        iterator_to_array(from_floe($path, filesystem: $memory)->extract(
            flow_context(config_builder()->backend($backend)->build()),
        ));

        static::assertGreaterThanOrEqual(1, $backend->decodes());
    }

    public function test_negative_offset_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->withOffset(-1);
    }

    public function test_schema_reads_footer_only(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://schema.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), $context);
        $loader->closure($context);

        $schema = from_floe($path, filesystem: $memory)->schema();

        static::assertNotNull($schema->findDefinition('id'));
    }

    public function test_schema_declares_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        static::assertSame(
            ['id', 'country'],
            from_floe(path('memory://parts/*/*.floe'), filesystem: $memory)->schema()->references()->names(),
        );
    }

    public function test_extract_fills_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeFilesContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $memory);
        $values = [];

        foreach ($extractor->extract(flow_context(config())) as $rows) {
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

        foreach ($extractor->extract(flow_context(config())) as $rows) {
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

        $generator = from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->extract(flow_context(config()));
        $generator->current();
        unset($generator);

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_every_reader_it_opens(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        foreach (from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->extract(
            flow_context(config()),
        ) as $_rows) {
        }

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_the_reader_it_skips_for_the_offset(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        foreach (from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)
            ->withOffset(1)
            ->extract(flow_context(config())) as $_rows) {
        }

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_extract_closes_the_reader_when_the_pipeline_stops(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeFilesContext::writePartitionedFiles($memory);

        $generator = from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->extract(flow_context(config()));

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

        $batches = iterator_to_array($extractor->extract(flow_context(config()), pathFilter: new RejectingFilter()));

        static::assertSame([], $batches);
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
        $loader->load(RowsMother::sequentialIds(7), $context);
        $loader->closure($context);

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
        $loader->load(RowsMother::sequentialIds(20), $context);
        $loader->closure($context);

        $sizes = [];

        foreach (from_floe($path, filesystem: $memory)->withBatchSize(7)->extract($context) as $rows) {
            $sizes[] = $rows->count();
        }

        static::assertLessThanOrEqual(7, max($sizes));
        static::assertSame(20, array_sum($sizes));
    }
}
