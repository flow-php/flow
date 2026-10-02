<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Engine\PhpParquetFileWriter;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AdaptiveParquetEngineTest extends TestCase
{
    public function test_rows_written_through_the_contract_read_back(): void
    {
        $filesystem = memory_filesystem();
        $engine = new AdaptiveParquetEngine();

        $engine->writeRows(
            $filesystem->writeTo(path('memory://out.parquet')),
            Schema::with(FlatColumn::int32('id')),
            Compressions::SNAPPY,
            new Options(),
            [['id' => 1], ['id' => 2]],
        );

        static::assertSame(
            [['id' => [1, 2]]],
            iterator_to_array(
                $engine->openForRead($filesystem->readFrom(path('memory://out.parquet')))->readColumns(
                    ['id'],
                    10,
                    null,
                    null,
                ),
                false,
            ),
        );
    }

    public function test_without_arrow_files_are_read_and_written_by_the_php_engine(): void
    {
        if (extension_loaded('arrow')) {
            static::markTestSkipped('This test requires arrow not to be loaded');
        }

        $engine = new AdaptiveParquetEngine();

        static::assertInstanceOf(
            PhpParquetFileReader::class,
            $engine->openForRead(new MemorySourceStream(MemoryParquetFile::threeRowGroups())),
        );
        static::assertInstanceOf(PhpParquetFileWriter::class, ParquetFileWriterMother::open(
            $engine,
            new StringDestinationStream(path('memory://out.parquet')),
        ));
    }

    #[RequiresPhpExtension('arrow')]
    public function test_with_arrow_files_are_read_and_written_by_arrow(): void
    {
        $engine = new AdaptiveParquetEngine();

        static::assertInstanceOf(
            RustParquetFileReader::class,
            $engine->openForRead(new MemorySourceStream(MemoryParquetFile::threeRowGroups())),
        );
        static::assertInstanceOf(RustParquetFileWriter::class, ParquetFileWriterMother::open(
            $engine,
            new StringDestinationStream(path('memory://out.parquet')),
        ));
    }

    public function test_big_endian_is_read_by_the_php_engine(): void
    {
        static::assertInstanceOf(
            PhpParquetFileReader::class,
            (new AdaptiveParquetEngine(ByteOrder::BIG_ENDIAN))->openForRead(
                new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ),
        );
    }
}
