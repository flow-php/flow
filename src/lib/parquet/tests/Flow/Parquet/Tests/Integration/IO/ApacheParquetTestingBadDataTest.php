<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\{ParquetEngine, Reader};
use PHPUnit\Framework\Attributes\{DataProvider, Group};

#[Group('apache-parquet-testing')]
class ApacheParquetTestingBadDataTest extends ParquetIntegrationTestCase
{
    private const string BAD_DATA_PATH = __DIR__ . '/Fixtures/parquet-testing/bad_data';

    public static function bad_data_fixture_with_engine_provider() : \Generator
    {
        if (!\is_dir(self::BAD_DATA_PATH)) {
            return;
        }

        foreach (self::engine_provider() as $engineName => [$engine]) {
            foreach (self::badDataFixtures() as $fixtureName => $fixturePath) {
                yield "{$engineName}/{$fixtureName}" => [$engine, $fixturePath];
            }
        }
    }

    /**
     * @return array<string, string>
     */
    public static function badDataFixtures() : array
    {
        return [
            'ARROW-GH-41317' => self::BAD_DATA_PATH . '/ARROW-GH-41317.parquet',
            'ARROW-GH-41321' => self::BAD_DATA_PATH . '/ARROW-GH-41321.parquet',
            'ARROW-GH-45185' => self::BAD_DATA_PATH . '/ARROW-GH-45185.parquet',
            'ARROW-RS-GH-6229-DICTHEADER' => self::BAD_DATA_PATH . '/ARROW-RS-GH-6229-DICTHEADER.parquet',
            'ARROW-RS-GH-6229-LEVELS' => self::BAD_DATA_PATH . '/ARROW-RS-GH-6229-LEVELS.parquet',
            'PARQUET-1481' => self::BAD_DATA_PATH . '/PARQUET-1481.parquet',
        ];
    }

    protected function setUp() : void
    {
        if (!\is_dir(self::BAD_DATA_PATH)) {
            static::markTestSkipped(
                'apache/parquet-testing submodule not initialized. '
                . 'Run: git submodule update --init'
            );
        }
    }

    #[DataProvider('bad_data_fixture_with_engine_provider')]
    public function test_bad_data_throws_exception(ParquetEngine $engine, string $path) : void
    {
        $this->expectException(\Throwable::class);

        $file = (new Reader(engine: $engine))->read($path);

        \iterator_to_array($file->values());
    }

    #[DataProvider('engine_provider')]
    public function test_graceful_handling_of_zero_bitwidth_dictionary(ParquetEngine $engine) : void
    {
        $file = (new Reader(engine: $engine))->read(self::BAD_DATA_PATH . '/ARROW-GH-43605.parquet');

        $count = 0;

        foreach ($file->values() as $row) {
            static::assertIsArray($row);
            $count++;
        }

        static::assertSame($file->metadata()->rowsNumber(), $count);
    }
}
