<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class ArrowParquetEngineTest extends TestCase
{
    public function test_constructor_throws_when_extension_not_loaded(): void
    {
        if (\extension_loaded('arrow')) {
            static::markTestSkipped('This test requires the arrow extension to NOT be loaded');
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('arrow extension is required');

        new ArrowParquetEngine();
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
