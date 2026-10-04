<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetSourceFileOpener;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Adapter\Parquet\Tests\Double\RefusingParquetEngine;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ParquetSourceFileOpenerTest extends FlowTestCase
{
    public function test_opens_the_file_narrowed_to_the_requested_columns(): void
    {
        $file = (new ParquetSourceFileOpener(
            new NativeLocalFilesystem(),
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            ['id'],
        ))->open(new SourceFile(ParquetSourceFileContext::fixture()));

        static::assertEquals(schema(int_schema('id')), $file->schema());

        $file->close();
    }

    public function test_a_stream_the_engine_refuses_is_closed(): void
    {
        $filesystem = new CountingFilesystem(new NativeLocalFilesystem());

        try {
            (new ParquetSourceFileOpener(
                $filesystem,
                new RefusingParquetEngine(),
                Options::default(),
                new SchemaConverter(),
                [],
            ))->open(new SourceFile(ParquetSourceFileContext::fixture()));
            static::fail('the engine must refuse the stream');
        } catch (RuntimeException) {
            static::assertSame(1, $filesystem->readFromCalls);
            static::assertSame(1, $filesystem->closedStreams());
        }
    }
}
