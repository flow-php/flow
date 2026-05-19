<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\Codec;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function strlen;

final class CodecTest extends TestCase
{
    #[Group('brotli-extension')]
    public function test_brotli(): void
    {
        if (!extension_loaded('brotli')) {
            static::markTestSkipped('The Brotli extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress(
            $codec->compress($data, Compressions::BROTLI),
            Compressions::BROTLI,
        ));
    }

    public function test_gzip(): void
    {
        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options())->set(Option::GZIP_COMPRESSION_LEVEL, 9));

        static::assertSame($data, $codec->decompress($codec->compress($data, Compressions::GZIP), Compressions::GZIP));
    }

    #[Group('lz4-extension')]
    public function test_lz4(): void
    {
        if (!extension_loaded('lz4')) {
            static::markTestSkipped('The lz4 extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress($codec->compress($data, Compressions::LZ4), Compressions::LZ4));
    }

    #[Group('lz4-extension')]
    public function test_lz4_raw(): void
    {
        if (!extension_loaded('lz4')) {
            static::markTestSkipped('The lz4 extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress(
            $codec->compress($data, Compressions::LZ4_RAW),
            Compressions::LZ4_RAW,
            strlen($data),
        ));
    }

    #[Group('snappy-extension')]
    public function test_snappy(): void
    {
        if (!extension_loaded('snappy')) {
            static::markTestSkipped('The snappy extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress(
            $codec->compress($data, Compressions::SNAPPY),
            Compressions::SNAPPY,
        ));
    }

    public function test_snappy_polyfill(): void
    {
        if (extension_loaded('snappy')) {
            static::markTestSkipped('The snappy extension is available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress(
            $codec->compress($data, Compressions::SNAPPY),
            Compressions::SNAPPY,
        ));
    }

    public function test_snappy_uncompressed(): void
    {
        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress(
            $codec->compress($data, Compressions::UNCOMPRESSED),
            Compressions::UNCOMPRESSED,
        ));
    }

    #[Group('zstd-extension')]
    public function test_zstd(): void
    {
        if (!extension_loaded('zstd')) {
            static::markTestSkipped('The Zstd extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec(new Options());

        static::assertSame($data, $codec->decompress($codec->compress($data, Compressions::ZSTD), Compressions::ZSTD));
    }
}
