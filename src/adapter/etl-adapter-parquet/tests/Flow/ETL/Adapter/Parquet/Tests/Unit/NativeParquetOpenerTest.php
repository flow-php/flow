<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\NativeParquetOpener;
use Flow\ETL\Adapter\Parquet\NativeParquetOpenSource;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Tests\Double\FailingReadSourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

#[RequiresPhpExtension('flow_php')]
#[RequiresPhpExtension('arrow')]
final class NativeParquetOpenerTest extends FlowTestCase
{
    public function test_sink_writes_native_columns(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $sink = (new NativeParquetOpener(Options::default()))->sink(
            $filesystem->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet($schema),
            Compressions::ZSTD,
            Options::default(),
        );

        $sink->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $sink->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_source_reads_the_native_file_the_file_was_opened_with(): void
    {
        $opener = new NativeParquetOpener(Options::default());
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem(), opener: $opener);

        static::assertEquals(new NativeParquetOpenSource($file->file->reader()->file()), $opener->source($file));
    }

    public function test_an_exception_the_stream_throws_while_opening_surfaces_as_itself(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 10, 4);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Reading "memory://groups.parquet" failed');

        (new NativeParquetOpener(Options::default()))->file(new FailingReadSourceStream($filesystem->readFrom(path(
            'memory://groups.parquet',
        ))));
    }
}
