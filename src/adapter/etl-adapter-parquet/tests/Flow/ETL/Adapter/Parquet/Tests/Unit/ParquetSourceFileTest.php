<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetSourceFileContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

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

        foreach ($source->batches(schema(int_schema('id')), 400, 100, 850, new PhpBackend()) as $rows) {
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
}
