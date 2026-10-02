<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\AdaptiveParquetOpenSource;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\RustIterator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Engine\RustParquetEngine;
use Generator;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class AdaptiveParquetOpenSourceTest extends FlowTestCase
{
    public function test_a_php_engine_file_reads_through_php(): void
    {
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem())->file;
        $source = new AdaptiveParquetOpenSource($file);

        static::assertInstanceOf(Generator::class, $source->batches(
            schema(int_schema('id')),
            10,
            null,
            null,
            new PhpBackend(),
        ));
        $source->close();
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_with_flow_php_a_rust_engine_file_reads_through_rust(): void
    {
        // the file closes its reader when released, so it outlives the source as ParquetSourceFile does
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem(), engine: new RustParquetEngine())->file;
        $source = new AdaptiveParquetOpenSource($file);

        static::assertInstanceOf(RustIterator::class, $source->batches(
            schema(int_schema('id')),
            10,
            null,
            null,
            new PhpBackend(),
        ));
        $source->close();
    }

    #[RequiresPhpExtension('arrow')]
    public function test_without_flow_php_a_rust_engine_file_reads_through_php(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php is loaded');
        }

        // the file closes its reader when released, so it outlives the source as ParquetSourceFile does
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem(), engine: new RustParquetEngine())->file;
        $source = new AdaptiveParquetOpenSource($file);

        static::assertInstanceOf(Generator::class, $source->batches(
            schema(int_schema('id')),
            10,
            null,
            null,
            new PhpBackend(),
        ));
        $source->close();
    }
}
