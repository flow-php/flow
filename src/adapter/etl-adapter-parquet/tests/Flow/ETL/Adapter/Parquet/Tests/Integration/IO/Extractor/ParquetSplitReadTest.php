<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\RecordingParquetEngine;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function array_filter;
use function extension_loaded;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function sort;

final class ParquetSplitReadTest extends FlowTestCase
{
    public function test_the_reader_reads_what_the_extractor_reads_before_and_after_a_rebuild(): void
    {
        $root = ReaderRoundTrip::scratch('parquet');
        ParquetFilesContext::writeRowGroups(native_local_filesystem(), $root->uri() . '/a.parquet', 200);
        ParquetFilesContext::writeRowGroups(native_local_filesystem(), $root->uri() . '/b.parquet', 50);
        $extractor = from_parquet($root->suffix('/*.parquet'))->withBatchSize(70);
        $reader = $extractor->reader();

        static::assertSame(ExtractedRows::of($extractor)->toArray(), ReaderRoundTrip::rows($reader, flow_context()));
        static::assertSame(
            ReaderRoundTrip::extractorBatches($extractor, flow_context()),
            ReaderRoundTrip::batches($reader, flow_context()),
        );
        static::assertSame(
            ReaderRoundTrip::rows($reader, flow_context()),
            ReaderRoundTrip::rows(DescribeRoundTrip::rebuilt($reader), flow_context()),
        );

        native_local_filesystem()->rm($root);
    }

    public function test_an_engine_without_configuration_does_not_describe(): void
    {
        $root = ReaderRoundTrip::scratch('parquet-engine');
        ParquetFilesContext::writeRowGroups(native_local_filesystem(), $root->uri() . '/a.parquet', 10);

        static::assertNull(
            from_parquet($root->suffix('/a.parquet'))->withEngine(new RecordingParquetEngine())->reader()->describe(),
        );
        static::assertNotNull(from_parquet($root->suffix('/a.parquet'))->reader()->describe());

        native_local_filesystem()->rm($root);
    }

    public function test_a_memory_filesystem_does_not_describe(): void
    {
        $memory = memory_filesystem();
        ParquetFilesContext::writeRowGroups($memory, 'memory://a.parquet', 10);

        static::assertNull(from_parquet(path('memory://a.parquet'), filesystem: $memory)->reader()->describe());
    }

    public function test_the_reader_declares_its_engines_extensions(): void
    {
        $root = ReaderRoundTrip::scratch('parquet-extensions');
        $expected = [
            'bcmath',
            'json',
            'zlib',
            ...array_filter(['brotli', 'lz4', 'snappy', 'zstd'], extension_loaded(...)),
        ];
        sort($expected);

        static::assertSame(
            $expected,
            from_parquet($root->suffix('/a.parquet'))
                ->withSchema(schema(int_schema('id')))
                ->reader()
                ->describe()
                ?->extensions(),
        );

        native_local_filesystem()->rm($root);
    }
}
