<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\RustIterator;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Reader\ReadPlan;
use Generator;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function extension_loaded;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ParquetSourceFileTest extends FlowTestCase
{
    public function test_close_releases_the_stream(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        ParquetSourceFileContext::over($filesystem)->close();

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_open_reads_the_batches_of_the_file(): void
    {
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem());
        $source = $file->open();
        $counts = [];

        foreach ($source->batches(
            schema(int_schema('id')),
            400,
            ReadPlan::forFilter($file->file->reader()->footer(), $file->file->indexes(), null, 100, 850),
            new PhpBackend(),
        ) as $rows) {
            $counts[] = $rows->count();
        }

        $source->close();

        static::assertSame([400, 400, 50], $counts);
    }

    public function test_schema_comes_from_the_file_footer(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            ParquetSourceFileContext::over(new NativeLocalFilesystem())->schema(),
        );
    }

    public function test_schema_is_narrowed_down_to_the_requested_columns(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            ParquetSourceFileContext::over(new NativeLocalFilesystem(), ['id'])->schema(),
        );
    }

    public function test_source_returns_the_listed_file(): void
    {
        static::assertEquals(
            new SourceFile(ParquetSourceFileContext::fixture()),
            ParquetSourceFileContext::over(new NativeLocalFilesystem())->source(),
        );
    }

    public function test_a_php_engine_file_opens_the_php_lane(): void
    {
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem());
        $source = $file->open();

        static::assertInstanceOf(Generator::class, $source->batches(
            schema(int_schema('id')),
            10,
            ReadPlan::forFilter($file->file->reader()->footer(), $file->file->indexes(), null),
            new PhpBackend(),
        ));
        $source->close();
    }

    #[RequiresPhpExtension('flow_php')]
    #[RequiresPhpExtension('arrow')]
    public function test_with_flow_php_a_rust_engine_file_opens_the_rust_lane(): void
    {
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem(), engine: new RustParquetEngine());
        $source = $file->open();

        static::assertInstanceOf(RustIterator::class, $source->batches(
            schema(int_schema('id')),
            10,
            ReadPlan::forFilter($file->file->reader()->footer(), $file->file->indexes(), null),
            new PhpBackend(),
        ));
        $source->close();
    }

    #[RequiresPhpExtension('arrow')]
    public function test_without_flow_php_a_rust_engine_file_opens_the_php_lane(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php is loaded');
        }

        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem(), engine: new RustParquetEngine());
        $source = $file->open();

        static::assertInstanceOf(Generator::class, $source->batches(
            schema(int_schema('id')),
            10,
            ReadPlan::forFilter($file->file->reader()->footer(), $file->file->indexes(), null),
            new PhpBackend(),
        ));
        $source->close();
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        ParquetSourceFileContext::over(
            new NativeLocalFilesystem(),
            extension: new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        )->open();
    }
}
