<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\Writer\StatisticsLevel;
use PHPUnit\Framework\TestCase;

final class OptionsConverterTest extends TestCase
{
    public function test_engine_options_default_without_options(): void
    {
        static::assertSame(['INT_96_AS_DATETIME' => true], OptionsConverter::toEngine(null));
    }

    public function test_engine_options_are_read_from_the_options(): void
    {
        static::assertSame(
            ['INT_96_AS_DATETIME' => false],
            OptionsConverter::toEngine((new Options())->set(Option::INT_96_AS_DATETIME, false)),
        );
    }

    public function test_arrow_write_batch_size_is_a_write_option(): void
    {
        static::assertSame(
            7,
            OptionsConverter::toExtension((new Options())->set(
                Option::ARROW_WRITE_BATCH_SIZE,
                7,
            ))['ARROW_WRITE_BATCH_SIZE'],
        );
        static::assertSame(
            (new Options())->getInt(Option::ARROW_WRITE_BATCH_SIZE),
            OptionsConverter::toExtension(new Options())['ARROW_WRITE_BATCH_SIZE'],
        );
    }

    public function test_dictionary_enabled_is_mapped(): void
    {
        static::assertFalse(
            OptionsConverter::toExtension((new Options())->set(
                Option::DICTIONARY_ENABLED,
                false,
            ))['DICTIONARY_ENABLED'],
        );
    }

    public function test_page_maximum_rows_count_is_mapped(): void
    {
        static::assertSame(
            50,
            OptionsConverter::toExtension((new Options())->set(
                Option::PAGE_MAXIMUM_ROWS_COUNT,
                50,
            ))['PAGE_MAXIMUM_ROWS_COUNT'],
        );
    }

    public function test_row_group_maximum_rows_count_is_mapped(): void
    {
        static::assertSame(
            300,
            OptionsConverter::toExtension((new Options())->set(
                Option::ROW_GROUP_MAXIMUM_ROWS_COUNT,
                300,
            ))['ROW_GROUP_MAXIMUM_ROWS_COUNT'],
        );
    }

    public function test_created_by_is_the_one_the_php_engine_writes(): void
    {
        static::assertSame(ParquetFile::createdBy(), OptionsConverter::toExtension(new Options())['CREATED_BY']);
    }

    public function test_statistics_truncate_length_is_mapped(): void
    {
        static::assertSame(
            8,
            OptionsConverter::toExtension((new Options())->set(
                Option::STATISTICS_TRUNCATE_LENGTH,
                8,
            ))['STATISTICS_TRUNCATE_LENGTH'],
        );
    }

    public function test_page_index_options_are_mapped_with_the_statistics_level_by_name(): void
    {
        $result = OptionsConverter::toExtension((new Options())->set(
            Option::STATISTICS_LEVEL,
            StatisticsLevel::CHUNK,
        )->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 16)->set(Option::PAGE_HEADER_STATISTICS, true));

        static::assertSame('CHUNK', $result['STATISTICS_LEVEL']);
        static::assertSame(16, $result['COLUMN_INDEX_TRUNCATE_LENGTH']);
        static::assertTrue($result['PAGE_HEADER_STATISTICS']);
        static::assertSame('PAGE', OptionsConverter::toExtension(new Options())['STATISTICS_LEVEL']);
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

    public function test_columns_bloom_filters_map_to_fpp_and_ndv(): void
    {
        $options = new Options();
        $options->set(Option::COLUMNS_BLOOM_FILTERS, [
            'id' => new BloomFilterOptions(0.01, 1000),
            'user.name' => new BloomFilterOptions(),
        ]);

        static::assertSame(
            ['id' => ['fpp' => 0.01, 'ndv' => 1000], 'user.name' => ['fpp' => 0.05, 'ndv' => null]],
            OptionsConverter::toExtension($options)['COLUMNS_BLOOM_FILTERS'],
        );
    }

    public function test_columns_bloom_filters_refuse_a_value_that_is_not_bloom_filter_options(): void
    {
        $options = new Options();
        $options->set(Option::COLUMNS_BLOOM_FILTERS, ['id' => 0.01]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Option COLUMNS_BLOOM_FILTERS takes BloomFilterOptions, got float for column "id"',
        );

        OptionsConverter::toExtension($options);
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

        static::assertArrayHasKey('STATISTICS_TRUNCATE_LENGTH', $result);
        static::assertArrayHasKey('GZIP_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('BROTLI_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('ZSTD_COMPRESSION_LEVEL', $result);
        static::assertArrayHasKey('ROW_GROUP_SIZE_BYTES', $result);
        static::assertArrayHasKey('PAGE_SIZE_BYTES', $result);
        static::assertTrue($result['DICTIONARY_ENABLED']);
        static::assertArrayHasKey('DICTIONARY_PAGE_SIZE', $result);
        static::assertArrayHasKey('WRITER_VERSION', $result);
        static::assertArrayNotHasKey('COLUMNS_COMPRESSIONS', $result);
        static::assertArrayNotHasKey('COLUMNS_ENCODINGS', $result);
        static::assertArrayNotHasKey('COLUMNS_BLOOM_FILTERS', $result);
    }
}
