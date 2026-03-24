<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\{ParquetEngine, Reader};
use PHPUnit\Framework\Attributes\{DataProvider, Group};

#[Group('apache-parquet-testing')]
class ApacheParquetTestingReadTest extends ParquetIntegrationTestCase
{
    private const string FIXTURES_PATH = __DIR__ . '/Fixtures/parquet-testing/data';

    public static function brotli_fixture_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        yield 'php/large_string_map.brotli' => [
            self::FIXTURES_PATH . '/large_string_map.brotli.parquet',
        ];
    }

    public static function concatenated_gzip_fixture_with_engine_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        foreach (self::engine_provider() as $engineName => [$engine]) {
            yield "{$engineName}/concatenated_gzip_members" => [
                $engine,
                self::FIXTURES_PATH . '/concatenated_gzip_members.parquet',
            ];
        }
    }

    public static function lz4_raw_fixture_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        foreach (self::engine_provider() as $engineName => [$engine]) {
            yield "{$engineName}/lz4_raw_compressed" => [
                $engine,
                self::FIXTURES_PATH . '/lz4_raw_compressed.parquet',
            ];
            yield "{$engineName}/lz4_raw_compressed_larger" => [
                $engine,
                self::FIXTURES_PATH . '/lz4_raw_compressed_larger.parquet',
            ];
        }
    }

    public static function php_only_readable_fixture_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        foreach (self::phpOnlyReadableFixtures() as $fixtureName => $fixturePath) {
            yield "php/{$fixtureName}" => [$fixturePath];
        }
    }

    /**
     * @return array<string, string>
     */
    public static function phpOnlyReadableFixtures() : array
    {
        return [
            'dict-page-offset-zero' => self::FIXTURES_PATH . '/dict-page-offset-zero.parquet',
            'fixed_length_byte_array' => self::FIXTURES_PATH . '/fixed_length_byte_array.parquet',
            'nation.dict-malformed' => self::FIXTURES_PATH . '/nation.dict-malformed.parquet',
        ];
    }

    public static function readable_fixture_with_engine_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        foreach (self::engine_provider() as $engineName => [$engine]) {
            foreach (self::readableFixtures() as $fixtureName => $fixturePath) {
                yield "{$engineName}/{$fixtureName}" => [$engine, $fixturePath];
            }
        }
    }

    /**
     * @return array<string, string>
     */
    public static function readableFixtures() : array
    {
        return [
            'alltypes_dictionary' => self::FIXTURES_PATH . '/alltypes_dictionary.parquet',
            'alltypes_plain' => self::FIXTURES_PATH . '/alltypes_plain.parquet',
            'alltypes_plain.snappy' => self::FIXTURES_PATH . '/alltypes_plain.snappy.parquet',
            'binary' => self::FIXTURES_PATH . '/binary.parquet',
            'binary_truncated_min_max' => self::FIXTURES_PATH . '/binary_truncated_min_max.parquet',
            'byte_array_decimal' => self::FIXTURES_PATH . '/byte_array_decimal.parquet',
            'column_chunk_key_value_metadata' => self::FIXTURES_PATH . '/column_chunk_key_value_metadata.parquet',
            'data_index_bloom_encoding_stats' => self::FIXTURES_PATH . '/data_index_bloom_encoding_stats.parquet',
            'data_index_bloom_encoding_with_length' => self::FIXTURES_PATH . '/data_index_bloom_encoding_with_length.parquet',
            'datapage_v1-snappy-compressed-checksum' => self::FIXTURES_PATH . '/datapage_v1-snappy-compressed-checksum.parquet',
            'datapage_v1-uncompressed-checksum' => self::FIXTURES_PATH . '/datapage_v1-uncompressed-checksum.parquet',
            'delta_binary_packed' => self::FIXTURES_PATH . '/delta_binary_packed.parquet',
            'fixed_length_decimal' => self::FIXTURES_PATH . '/fixed_length_decimal.parquet',
            'fixed_length_decimal_legacy' => self::FIXTURES_PATH . '/fixed_length_decimal_legacy.parquet',
            'int32_decimal' => self::FIXTURES_PATH . '/int32_decimal.parquet',
            'int32_with_null_pages' => self::FIXTURES_PATH . '/int32_with_null_pages.parquet',
            'int64_decimal' => self::FIXTURES_PATH . '/int64_decimal.parquet',
            'list_columns' => self::FIXTURES_PATH . '/list_columns.parquet',
            'map_no_value' => self::FIXTURES_PATH . '/map_no_value.parquet',
            'nan_in_stats' => self::FIXTURES_PATH . '/nan_in_stats.parquet',
            'nested_lists.snappy' => self::FIXTURES_PATH . '/nested_lists.snappy.parquet',
            'nested_maps.snappy' => self::FIXTURES_PATH . '/nested_maps.snappy.parquet',
            'nested_structs.rust' => self::FIXTURES_PATH . '/nested_structs.rust.parquet',
            'nonnullable.impala' => self::FIXTURES_PATH . '/nonnullable.impala.parquet',
            'null_list' => self::FIXTURES_PATH . '/null_list.parquet',
            'nullable.impala' => self::FIXTURES_PATH . '/nullable.impala.parquet',
            'nulls.snappy' => self::FIXTURES_PATH . '/nulls.snappy.parquet',
            'overflow_i16_page_cnt' => self::FIXTURES_PATH . '/overflow_i16_page_cnt.parquet',
            'page_v2_empty_compressed' => self::FIXTURES_PATH . '/page_v2_empty_compressed.parquet',
            'plain-dict-uncompressed-checksum' => self::FIXTURES_PATH . '/plain-dict-uncompressed-checksum.parquet',
            'repeated_no_annotation' => self::FIXTURES_PATH . '/repeated_no_annotation.parquet',
            'rle-dict-snappy-checksum' => self::FIXTURES_PATH . '/rle-dict-snappy-checksum.parquet',
            'single_nan' => self::FIXTURES_PATH . '/single_nan.parquet',
            'sort_columns' => self::FIXTURES_PATH . '/sort_columns.parquet',
            'unknown-logical-type' => self::FIXTURES_PATH . '/unknown-logical-type.parquet',
        ];
    }

    protected function setUp() : void
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            static::markTestSkipped(
                'apache/parquet-testing submodule not initialized. '
                . 'Run: git submodule update --init'
            );
        }
    }

    #[DataProvider('engine_provider')]
    public function test_reading_alltypes_plain(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::FIXTURES_PATH . '/alltypes_plain.parquet');

        $columnNames = \array_map(
            static fn ($c) => $c->name(),
            $file->schema()->columns(),
        );

        static::assertContains('id', $columnNames);
        static::assertContains('bool_col', $columnNames);
        static::assertContains('tinyint_col', $columnNames);
        static::assertContains('smallint_col', $columnNames);
        static::assertContains('int_col', $columnNames);
        static::assertContains('bigint_col', $columnNames);
        static::assertContains('float_col', $columnNames);
        static::assertContains('double_col', $columnNames);
        static::assertContains('date_string_col', $columnNames);
        static::assertContains('string_col', $columnNames);
        static::assertContains('timestamp_col', $columnNames);

        $rows = \iterator_to_array($file->values());

        static::assertSame(8, \count($rows));
    }

    #[DataProvider('brotli_fixture_provider')]
    public function test_reading_brotli_fixture(string $path) : void
    {
        if (!\extension_loaded('brotli')) {
            static::markTestSkipped('The Brotli extension is not available');
        }

        $file = (new Reader(engine: new PhpParquetEngine()))->read($path);
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }

    #[DataProvider('readable_fixture_with_engine_provider')]
    public function test_reading_fixture(ParquetEngine $engine, string $path) : void
    {
        $file = (new Reader(engine: $engine))->read($path);
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));
        static::assertGreaterThanOrEqual(0, $metadata->rowsNumber());

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }

    #[DataProvider('concatenated_gzip_fixture_with_engine_provider')]
    public function test_reading_gzip_fixture(ParquetEngine $engine, string $path) : void
    {
        $file = (new Reader(engine: $engine))->read($path);
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int96_timestamps(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::FIXTURES_PATH . '/int96_from_spark.parquet');
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }

    #[DataProvider('lz4_raw_fixture_provider')]
    public function test_reading_lz4_raw_fixture(ParquetEngine $engine, string $path) : void
    {
        if (!\extension_loaded('lz4')) {
            static::markTestSkipped('The LZ4 extension is not available');
        }

        $file = (new Reader(engine: $engine))->read($path);
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_nested_lists(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::FIXTURES_PATH . '/nested_lists.snappy.parquet');

        $rows = \iterator_to_array($file->values());

        static::assertGreaterThan(0, \count($rows));
        static::assertArrayHasKey('a', $rows[0]);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_nested_maps(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::FIXTURES_PATH . '/nested_maps.snappy.parquet');

        $rows = \iterator_to_array($file->values());

        static::assertGreaterThan(0, \count($rows));
        static::assertArrayHasKey('a', $rows[0]);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_nulls(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::FIXTURES_PATH . '/nulls.snappy.parquet');

        $rows = \iterator_to_array($file->values());

        static::assertSame($file->metadata()->rowsNumber(), \count($rows));

        $containsNull = static function (mixed $value) use (&$containsNull) : bool {
            if ($value === null) {
                return true;
            }

            if (\is_array($value)) {
                foreach ($value as $v) {
                    if ($containsNull($v)) {
                        return true;
                    }
                }
            }

            return false;
        };

        $hasNull = false;

        foreach ($rows as $row) {
            if ($containsNull($row)) {
                $hasNull = true;

                break;
            }
        }

        static::assertTrue($hasNull, 'Expected at least one null value in nulls.snappy.parquet');
    }

    #[DataProvider('php_only_readable_fixture_provider')]
    public function test_reading_php_only_fixture(string $path) : void
    {
        $file = (new Reader(engine: new PhpParquetEngine()))->read($path);
        $metadata = $file->metadata();

        static::assertGreaterThan(0, \count($metadata->schema()->columns()));
        static::assertGreaterThanOrEqual(0, $metadata->rowsNumber());

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($metadata->rowsNumber(), $count);
    }
}
