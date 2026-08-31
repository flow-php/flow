<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Floe\Tests\Context\FloeEngineContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function iterator_to_array;

final class FloeExtractorTest extends TestCase
{
    public function test_change_limit_to_zero_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->changeLimit(0);
    }

    public function test_default_filter_keeps_only_files(): void
    {
        static::assertInstanceOf(
            OnlyFiles::class,
            from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->filter(),
        );
    }

    public function test_extract_adds_input_file_uri_when_configured(): void
    {
        $context = flow_context(config_builder()->build());
        $memory = memory_filesystem();
        $path = path('memory://input-uri.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory)->withMetadataColumns(true);
        $batches = iterator_to_array($extractor->extract($context));

        static::assertSame($path->uri(), $batches[0]->first()->get('_input_file_uri'));
    }

    public function test_extract_applies_offset_within_a_file(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://offset-within.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path, filesystem: $memory)->withOffset(1)->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->get('id');
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
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory);
        $extractor->changeLimit(2);

        $ids = [];

        foreach ($extractor->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->get('id');
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
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->closure($context);

        $batches = iterator_to_array(from_floe($path, filesystem: $memory)->extract($context));

        static::assertSame(['id'], $batches[0]->first()->names());
    }

    public function test_extract_reads_rows(): void
    {
        $context = flow_context(config());
        $memory = memory_filesystem();
        $path = path('memory://read.floe');

        $loader = to_floe($path, filesystem: $memory);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path, filesystem: $memory)->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->get('id');
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
            $loader->load(
                rows(schema(int_schema('id')), row(['id' => $values[0]]), row(['id' => $values[1]])),
                $context,
            );
            $loader->closure($context);
        }

        $ids = [];

        foreach (from_floe(path('memory://skip-files/*.floe'), filesystem: $memory)
            ->withOffset(2)
            ->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->get('id');
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
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])), $context);
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
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->closure($context);

        $extractor = from_floe($path, filesystem: $memory)
            ->withMetadataColumns(true)
            ->withSchema(schema(int_schema('id')));

        $batches = iterator_to_array($extractor->extract($context));

        static::assertSame($extractor->schema()->references()->names(), $batches[0]->first()->names());
        static::assertSame($path->uri(), $batches[0]->first()->get('_input_file_uri'));
    }

    public function test_is_limited_reflects_change_limit(): void
    {
        $extractor = from_floe(path('memory://x.floe'), filesystem: memory_filesystem());

        static::assertFalse($extractor->isLimited());

        $extractor->changeLimit(5);

        static::assertTrue($extractor->isLimited());
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
        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), $context);
        $loader->closure($context);

        $schema = from_floe($path, filesystem: $memory)->schema();

        static::assertNotNull($schema->findDefinition('id'));
    }

    public function test_schema_declares_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeEngineContext::writePartitionedFiles($memory);

        static::assertSame(
            ['id', 'country'],
            from_floe(path('memory://parts/*/*.floe'), filesystem: $memory)->schema()->references()->names(),
        );
    }

    public function test_extract_fills_partition_columns_from_the_path(): void
    {
        $memory = memory_filesystem();
        FloeEngineContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $memory);
        $values = [];

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows as $row) {
                $values[] = $row->toArray();
            }
        }

        static::assertSame([['id' => 2, 'country' => 'DE'], ['id' => 1, 'country' => 'PL']], $values);
    }

    public function test_schema_opens_only_the_first_file(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeEngineContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->schema();

        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_schema_is_memoised(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeEngineContext::writePartitionedFiles($memory);

        $extractor = from_floe(path('memory://parts/*/*.floe'), filesystem: $counting);

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(1, $counting->readFromCalls);
    }

    public function test_union_by_name_opens_every_file(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeEngineContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->unionByName()->schema();

        static::assertSame(2, $counting->readFromCalls);
    }

    public function test_schema_closes_every_reader_it_opens(): void
    {
        $counting = new CountingFilesystem($memory = memory_filesystem());
        FloeEngineContext::writePartitionedFiles($memory);

        from_floe(path('memory://parts/*/*.floe'), filesystem: $counting)->unionByName()->schema();

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_source_returns_path(): void
    {
        static::assertSame(
            'memory://x.floe',
            from_floe(path('memory://x.floe'), filesystem: memory_filesystem())->source()->uri(),
        );
    }

    public function test_with_path_filter_composes_filters(): void
    {
        $extractor = from_floe(path('memory://x.floe'), filesystem: memory_filesystem())
            ->withPathFilter(new OnlyFiles())
            ->withPathFilter(new OnlyFiles());

        static::assertInstanceOf(Filters::class, $extractor->filter());
    }
}
