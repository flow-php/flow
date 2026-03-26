<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\{generate_random_int, generate_random_string};
use Faker\Factory;
use Flow\Parquet\{Consts, Reader, Writer};
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\Attributes\{DataProvider, Group};

class CompressionTest extends ParquetIntegrationTestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    #[Group('brotli-extension')]
    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_brotli_compression(ParquetEngine $engine) : void
    {
        if (!\extension_loaded('brotli')) {
            static::markTestSkipped('The Brotli extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::BROTLI, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_gzip_compression(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::GZIP, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[Group('lz4-extension')]
    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_lz4_compression(ParquetEngine $engine) : void
    {
        if (!\extension_loaded('lz4')) {
            static::markTestSkipped('The lz4 extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::LZ4, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[Group('lz4-extension')]
    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_lz4_raw_compression(ParquetEngine $engine) : void
    {
        if (!\extension_loaded('lz4')) {
            static::markTestSkipped('The lz4 extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::LZ4_RAW, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[Group('snappy-extension')]
    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_snappy_compression(ParquetEngine $engine) : void
    {
        if (!\extension_loaded('snappy')) {
            static::markTestSkipped('The snappy extension is not available');
        }

        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::SNAPPY, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_snappy_polyfill(ParquetEngine $engine) : void
    {
        if (\extension_loaded('snappy')) {
            static::markTestSkipped('The snappy extension is available');
        }

        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::SNAPPY, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_uncompressed_compression(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::UNCOMPRESSED, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }

    #[Group('zstd-extension')]
    #[DataProvider('engine_provider')]
    public function test_writing_and_reading_file_with_zstd_compression(ParquetEngine $engine) : void
    {
        if (!\extension_loaded('zstd')) {
            static::markTestSkipped('The Zstd extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(compression: Compressions::ZSTD, engine: $engine);

        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                    'boolean' => $faker->boolean,
                    'string' => $faker->text(150),
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'list_of_int' => \array_map(
                        static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        \range(1, generate_random_int(2, 10))
                    ),
                    'list_of_string' => \array_map(
                        static fn ($i) => $faker->text(10),
                        \range(1, generate_random_int(2, 10))
                    ),
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
        static::assertFileExists($path);
        \unlink($path);
    }
}
