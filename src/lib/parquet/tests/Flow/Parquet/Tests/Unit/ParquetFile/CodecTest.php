<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\Parquet\ParquetFile\{Codec, Compressions};
use Flow\Parquet\{Option, Options};
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

final class CodecTest extends TestCase
{
    #[Group('brotli-extension')]
    public function test_brotli() : void
    {
        if (!\extension_loaded('brotli')) {
            self::markTestSkipped('The Brotli extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::BROTLI), Compressions::BROTLI)
        );
    }

    public function test_gzip() : void
    {
        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options())->set(Option::GZIP_COMPRESSION_LEVEL, 9));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::GZIP), Compressions::GZIP)
        );
    }

    #[Group('lz4-extension')]
    public function test_lz4() : void
    {
        if (!\extension_loaded('lz4')) {
            self::markTestSkipped('The lz4 extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::LZ4), Compressions::LZ4)
        );
    }

    #[Group('lz4-extension')]
    public function test_lz4_raw() : void
    {
        if (!\extension_loaded('lz4')) {
            self::markTestSkipped('The lz4 extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::LZ4_RAW), Compressions::LZ4_RAW)
        );
    }

    #[Group('snappy-extension')]
    public function test_snappy() : void
    {
        if (!\extension_loaded('snappy')) {
            self::markTestSkipped('The snappy extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::SNAPPY), Compressions::SNAPPY)
        );
    }

    public function test_snappy_polyfill() : void
    {
        if (\extension_loaded('snappy')) {
            self::markTestSkipped('The snappy extension is available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::SNAPPY), Compressions::SNAPPY)
        );
    }

    public function test_snappy_uncompressed() : void
    {
        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::UNCOMPRESSED), Compressions::UNCOMPRESSED)
        );
    }

    #[Group('zstd-extension')]
    public function test_zstd() : void
    {
        if (!\extension_loaded('zstd')) {
            self::markTestSkipped('The Zstd extension is not available');
        }

        $data = 'this is some test data to be compressed';

        $codec = new Codec((new Options()));

        self::assertSame(
            $data,
            $codec->decompress($codec->compress($data, Compressions::ZSTD), Compressions::ZSTD)
        );
    }
}
