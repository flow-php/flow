<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

final class CompressionTest extends ParquetIntegrationTestCase
{
    public function test_writing_and_reading_file_with_gzip_compression() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

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
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'struct' => [
                        'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                        'boolean' => $faker->boolean,
                        'string' => $faker->text(150),
                        'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        'list_of_int' => \array_map(
                            static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            \range(1, \random_int(2, 10))
                        ),
                        'list_of_string' => \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, \random_int(2, 10))
                        ),
                    ],
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_and_reading_file_with_snappy_compression() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

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
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'struct' => [
                        'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                        'boolean' => $faker->boolean,
                        'string' => $faker->text(150),
                        'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        'list_of_int' => \array_map(
                            static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            \range(1, \random_int(2, 10))
                        ),
                        'list_of_string' => \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, \random_int(2, 10))
                        ),
                    ],
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_and_reading_file_with_uncompressed_compression() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

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
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'struct' => [
                        'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                        'boolean' => $faker->boolean,
                        'string' => $faker->text(150),
                        'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        'list_of_int' => \array_map(
                            static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            \range(1, \random_int(2, 10))
                        ),
                        'list_of_string' => \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, \random_int(2, 10))
                        ),
                    ],
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }
}
