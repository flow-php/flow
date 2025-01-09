<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\{generate_random_int, generate_random_string};
use Faker\Factory;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\{Consts, Reader, Writer};
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

final class CompressionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    #[Group('brotli-extension')]
    public function test_writing_and_reading_file_with_brotli_compression() : void
    {
        if (!\extension_loaded('brotli')) {
            self::markTestSkipped('The Brotli extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::BROTLI);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_and_reading_file_with_gzip_compression() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::GZIP);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    #[Group('lz4-extension')]
    public function test_writing_and_reading_file_with_lz4_compression() : void
    {
        if (!\extension_loaded('lz4')) {
            self::markTestSkipped('The lz4 extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::LZ4);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    #[Group('lz4-extension')]
    public function test_writing_and_reading_file_with_lz4_raw_compression() : void
    {
        if (!\extension_loaded('lz4')) {
            self::markTestSkipped('The lz4 extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::LZ4_RAW);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    #[Group('snappy-extension')]
    public function test_writing_and_reading_file_with_snappy_compression() : void
    {
        if (!\extension_loaded('snappy')) {
            self::markTestSkipped('The snappy extension is not available');
        }

        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::SNAPPY);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_and_reading_file_with_snappy_polyfill() : void
    {
        if (\extension_loaded('snappy')) {
            self::markTestSkipped('The snappy extension is available');
        }

        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::SNAPPY);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_and_reading_file_with_uncompressed_compression() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::UNCOMPRESSED);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    #[Group('zstd-extension')]
    public function test_writing_and_reading_file_with_zstd_compression() : void
    {
        if (!\extension_loaded('zstd')) {
            self::markTestSkipped('The Zstd extension is not available');
        }

        $path = \sys_get_temp_dir() . '/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(Compressions::ZSTD);

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }
}
