<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\EngineParquetOpener;
use Flow\ETL\Adapter\Parquet\EngineParquetOpenSource;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class EngineParquetOpenerTest extends FlowTestCase
{
    public function test_sink_writes_through_the_engine(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'));
        $sink = (new EngineParquetOpener(new PhpParquetEngine(), Options::default()))->sink(
            $filesystem->writeTo(path('memory://out.parquet')),
            (new SchemaConverter())->toParquet($schema),
            Compressions::SNAPPY,
            Options::default(),
        );

        $sink->write(array_to_rows([['id' => 1], ['id' => 2]], $schema));
        $sink->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            ParquetFilesContext::phpEngineValues($filesystem, 'memory://out.parquet'),
        );
    }

    public function test_source_reads_the_file_through_its_parquet_file(): void
    {
        $file = ParquetSourceFileContext::over(new NativeLocalFilesystem());

        static::assertEquals(
            new EngineParquetOpenSource($file->file),
            (new EngineParquetOpener(new PhpParquetEngine(), Options::default()))->source($file),
        );
    }
}
