<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use PHPUnit\Framework\TestCase;

final class OptionsConverterTest extends TestCase
{
    public function test_arrow_batch_size_is_excluded_when_null(): void
    {
        $options = new Options();
        $options->set(Option::ARROW_BATCH_SIZE, null);

        $result = OptionsConverter::toExtension($options);

        static::assertArrayNotHasKey('BATCH_SIZE', $result);
    }

    public function test_arrow_batch_size_is_included_when_set(): void
    {
        $options = new Options();
        $options->set(Option::ARROW_BATCH_SIZE, 512);

        $result = OptionsConverter::toExtension($options);

        static::assertSame(512, $result['BATCH_SIZE']);
    }

    public function test_columns_compressions_maps_enum_names(): void
    {
        $options = new Options();
        $options->set(Option::COLUMNS_COMPRESSIONS, [
            'col_a' => Compressions::SNAPPY,
            'col_b' => Compressions::GZIP,
        ]);

        $result = OptionsConverter::toExtension($options);

        static::assertSame(['col_a' => 'SNAPPY', 'col_b' => 'GZIP'], $result['COLUMNS_COMPRESSIONS']);
    }

    public function test_columns_encodings_maps_enum_names(): void
    {
        $options = new Options();
        $options->set(Option::COLUMNS_ENCODINGS, [
            'col_a' => Encodings::PLAIN,
            'col_b' => Encodings::RLE_DICTIONARY,
        ]);

        $result = OptionsConverter::toExtension($options);

        static::assertSame(['col_a' => 'PLAIN', 'col_b' => 'RLE_DICTIONARY'], $result['COLUMNS_ENCODINGS']);
    }

    public function test_custom_compression_levels(): void
    {
        $options = new Options();
        $options->set(Option::GZIP_COMPRESSION_LEVEL, 5);
        $options->set(Option::BROTLI_COMPRESSION_LEVEL, 8);
        $options->set(Option::ZSTD_COMPRESSION_LEVEL, 10);

        $result = OptionsConverter::toExtension($options);

        static::assertSame(5, $result['GZIP_COMPRESSION_LEVEL']);
        static::assertSame(8, $result['BROTLI_COMPRESSION_LEVEL']);
        static::assertSame(10, $result['ZSTD_COMPRESSION_LEVEL']);
    }

    public function test_default_options_produces_expected_keys(): void
    {
        $result = OptionsConverter::toExtension(new Options());

        static::assertArrayHasKey('BATCH_SIZE', $result);
        static::assertArrayHasKey('GZIP_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('BROTLI_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('ZSTD_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('ROW_GROUP_SIZE_BYTES', $result);
        static::assertArrayHasKey('PAGE_SIZE_BYTES', $result);
        static::assertArrayHasKey('DICTIONARY_PAGE_SIZE', $result);
        static::assertArrayHasKey('WRITER_VERSION', $result);
        static::assertArrayNotHasKey('COLUMNS_COMPRESSIONS', $result);
        static::assertArrayNotHasKey('COLUMNS_ENCODINGS', $result);
    }
}
