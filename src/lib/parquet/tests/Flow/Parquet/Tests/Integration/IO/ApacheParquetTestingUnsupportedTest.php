<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\{ArrowParquetEngine, PhpParquetEngine};
use Flow\Parquet\{ParquetEngine, Reader};
use PHPUnit\Framework\Attributes\{DataProvider, Group};

#[Group('apache-parquet-testing')]
class ApacheParquetTestingUnsupportedTest extends ParquetIntegrationTestCase
{
    private const string FIXTURES_PATH = __DIR__ . '/Fixtures/parquet-testing/data';

    public static function unsupported_arrow_fixture_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        if (!\extension_loaded('arrow')) {
            return;
        }

        $engine = new ArrowParquetEngine();

        foreach (self::unsupportedArrowFixtures() as $fixtureName => $fixturePath) {
            yield "arrow/{$fixtureName}" => [$engine, $fixturePath];
        }
    }

    public static function unsupported_php_fixture_provider() : \Generator
    {
        if (!\is_dir(self::FIXTURES_PATH)) {
            return;
        }

        $engine = new PhpParquetEngine();

        foreach (self::unsupportedPhpFixtures() as $fixtureName => $fixturePath) {
            yield "php/{$fixtureName}" => [$engine, $fixturePath];
        }
    }

    /**
     * @return array<string, string>
     */
    public static function unsupportedArrowFixtures() : array
    {
        return [
            'dict-page-offset-zero' => self::FIXTURES_PATH . '/dict-page-offset-zero.parquet',
            'fixed_length_byte_array' => self::FIXTURES_PATH . '/fixed_length_byte_array.parquet',
            'large_string_map.brotli' => self::FIXTURES_PATH . '/large_string_map.brotli.parquet',
            'nation.dict-malformed' => self::FIXTURES_PATH . '/nation.dict-malformed.parquet',
        ];
    }

    /**
     * @return array<string, string>
     */
    public static function unsupportedPhpFixtures() : array
    {
        return [
            'byte_stream_split.zstd' => self::FIXTURES_PATH . '/byte_stream_split.zstd.parquet',
            'byte_stream_split_extended.gzip' => self::FIXTURES_PATH . '/byte_stream_split_extended.gzip.parquet',
            'delta_byte_array' => self::FIXTURES_PATH . '/delta_byte_array.parquet',
            'delta_length_byte_array' => self::FIXTURES_PATH . '/delta_length_byte_array.parquet',
            'datapage_v2.snappy' => self::FIXTURES_PATH . '/datapage_v2.snappy.parquet',
            'datapage_v2_empty_datapage.snappy' => self::FIXTURES_PATH . '/datapage_v2_empty_datapage.snappy.parquet',
            'hadoop_lz4_compressed' => self::FIXTURES_PATH . '/hadoop_lz4_compressed.parquet',
            'hadoop_lz4_compressed_larger' => self::FIXTURES_PATH . '/hadoop_lz4_compressed_larger.parquet',
            'delta_encoding_optional_column' => self::FIXTURES_PATH . '/delta_encoding_optional_column.parquet',
            'delta_encoding_required_column' => self::FIXTURES_PATH . '/delta_encoding_required_column.parquet',
            'old_list_structure' => self::FIXTURES_PATH . '/old_list_structure.parquet',
            'rle_boolean_encoding' => self::FIXTURES_PATH . '/rle_boolean_encoding.parquet',
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

    #[DataProvider('unsupported_arrow_fixture_provider')]
    public function test_arrow_engine_throws_on_unsupported_fixture(ParquetEngine $engine, string $path) : void
    {
        $this->expectException(\Throwable::class);

        $file = (new Reader(engine: $engine))->read($path);

        \iterator_to_array($file->values());
    }

    #[DataProvider('unsupported_php_fixture_provider')]
    public function test_php_engine_throws_on_unsupported_fixture(ParquetEngine $engine, string $path) : void
    {
        $this->expectException(\Throwable::class);

        $file = (new Reader(engine: $engine))->read($path);

        \iterator_to_array($file->values());
    }
}
