<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\ArrowParquetFileReader;
use Flow\Parquet\Engine\NativeParquetFileReader;
use Flow\Parquet\Engine\NativeParquetFileWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Filesystem\DSL\path;
use function restore_error_handler;
use function set_error_handler;

use const E_USER_DEPRECATED;

final class ArrowParquetEngineTest extends TestCase
{
    public function test_constructor_throws_without_flow_php_and_arrow(): void
    {
        if (extension_loaded('flow_php') || extension_loaded('arrow')) {
            static::markTestSkipped('This test requires neither flow_php nor arrow to be loaded');
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('ArrowParquetEngine requires the flow_php extension (flow-php/flow-php-ext).');

        new ArrowParquetEngine();
    }

    #[RequiresPhpExtension('flow_php')]
    public function test_with_flow_php_files_are_read_and_written_through_it(): void
    {
        $engine = new ArrowParquetEngine();

        static::assertInstanceOf(
            NativeParquetFileReader::class,
            $engine->openForRead(new MemorySourceStream(MemoryParquetFile::threeRowGroups())),
        );
        static::assertInstanceOf(NativeParquetFileWriter::class, ParquetFileWriterMother::open(
            $engine,
            new StringDestinationStream(path('memory://out.parquet')),
        ));
    }

    #[RequiresPhpExtension('arrow')]
    public function test_with_arrow_only_files_are_read_through_arrow_with_a_deprecation(): void
    {
        if (extension_loaded('flow_php')) {
            static::markTestSkipped('This test requires flow_php NOT to be loaded');
        }

        $deprecations = [];
        set_error_handler(static function (int $level, string $message) use (&$deprecations): bool {
            $deprecations[] = [$level, $message];

            return true;
        });

        try {
            $engine = new ArrowParquetEngine();
        } finally {
            restore_error_handler();
        }

        static::assertSame(
            [[
                E_USER_DEPRECATED,
                'Parquet through the arrow extension is deprecated and will be removed; install the flow_php '
                    . 'extension (flow-php/flow-php-ext), which ArrowParquetEngine uses when it is loaded.',
            ]],
            $deprecations,
        );
        static::assertInstanceOf(
            ArrowParquetFileReader::class,
            $engine->openForRead(new MemorySourceStream(MemoryParquetFile::threeRowGroups())),
        );
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
