<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\ArrowParquetFileReader;
use Flow\Parquet\Engine\ArrowParquetFileWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Filesystem\DSL\path;

final class ArrowParquetEngineTest extends TestCase
{
    public function test_constructor_throws_without_arrow(): void
    {
        if (extension_loaded('arrow')) {
            static::markTestSkipped('This test requires arrow not to be loaded');
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('ArrowParquetEngine requires the arrow extension (flow-php/arrow-ext).');

        new ArrowParquetEngine();
    }

    #[RequiresPhpExtension('arrow')]
    public function test_files_are_read_and_written_through_arrow(): void
    {
        $engine = new ArrowParquetEngine();

        static::assertInstanceOf(
            ArrowParquetFileReader::class,
            $engine->openForRead(new MemorySourceStream(MemoryParquetFile::threeRowGroups())),
        );
        static::assertInstanceOf(ArrowParquetFileWriter::class, ParquetFileWriterMother::open(
            $engine,
            new StringDestinationStream(path('memory://out.parquet')),
        ));
    }

    #[TestWith([Compressions::UNCOMPRESSED, 'UNCOMPRESSED'])]
    #[TestWith([Compressions::SNAPPY, 'SNAPPY'])]
    #[TestWith([Compressions::GZIP, 'GZIP'])]
    #[TestWith([Compressions::BROTLI, 'BROTLI'])]
    #[TestWith([Compressions::LZ4, 'LZ4_RAW'])]
    #[TestWith([Compressions::LZ4_RAW, 'LZ4_RAW'])]
    #[TestWith([Compressions::ZSTD, 'ZSTD'])]
    public function test_map_compression(Compressions $input, string $expected): void
    {
        static::assertSame($expected, ArrowParquetEngine::mapCompression($input));
    }

    public function test_map_compression_throws_for_lzo(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('LZO compression is not supported');

        ArrowParquetEngine::mapCompression(Compressions::LZO);
    }
}
