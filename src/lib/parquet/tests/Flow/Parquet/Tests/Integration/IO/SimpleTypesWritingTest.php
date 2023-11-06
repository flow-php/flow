<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

final class SimpleTypesWritingTest extends TestCase
{
    public function test_writing_bool_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::boolean('boolean'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'boolean' => (bool) $i % 2 == 0,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_bool_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::boolean('boolean'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'boolean' => $i % 2 == 0 ? (bool) \random_int(0, 1) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_date_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::date('date'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'date' => \DateTimeImmutable::createFromMutable($faker->dateTimeThisYear)->setTime(0, 0, 0, 0),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_date_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::date('date'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'date' => $i % 2 === 0 ? \DateTimeImmutable::createFromMutable($faker->dateTimeThisYear)->setTime(0, 0, 0, 0) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_decimal_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::decimal('decimal'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'decimal' => \round($faker->randomFloat(5), 2),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_decimal_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::decimal('decimal'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'decimal' => $i % 2 === 0 ? \round($faker->randomFloat(5), 2) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_double_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::double('double'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'double' => $faker->randomFloat(),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_double_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::double('double'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'double' => $i % 2 === 0 ? $faker->randomFloat() : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_enum_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::enum('enum'));

        $enum = ['A', 'B', 'C', 'D'];

        $inputData = \array_merge(...\array_map(static function (int $i) use ($enum) : array {
            return [
                [
                    'enum' => $enum[\random_int(0, 3)],
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_float_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::float('float'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'float' => 10.25,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_float_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::float('float'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'float' => $i % 2 === 0 ? 10.25 : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_int32_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::int32('int32'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_int32_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::int32('int32'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'int32' => $i % 2 === 0 ? $faker->numberBetween(0, Consts::PHP_INT32_MAX) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );

        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_int64() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::int64('int64'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_int64_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::int64('int64'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'int64' => $i % 2 === 0 ? $faker->numberBetween(0, Consts::PHP_INT64_MAX) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_json_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::json('json'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'json' => \json_encode(['street' => $faker->streetName, 'city' => $faker->city, 'country' => $faker->country, 'zip' => $faker->postcode], JSON_THROW_ON_ERROR),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_json_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::json('json'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'json' => $i % 2 === 0
                        ? \json_encode(['street' => $faker->streetName, 'city' => $faker->city, 'country' => $faker->country, 'zip' => $faker->postcode], JSON_THROW_ON_ERROR)
                        : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_string_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::string('string'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'string' => $faker->text(50),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_string_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::string('string'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'string' => $i % 2 === 0 ? $faker->text(50) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_time_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::time('time'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'time' => (new \DateTimeImmutable('2023-01-01 00:00:00 UTC'))->diff(new \DateTimeImmutable('2023-01-01 15:45:00 UTC')),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_time_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::time('time'));

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'time' => $i % 2 === 0 ? (new \DateTimeImmutable('2023-01-01 00:00:00 UTC'))->diff(new \DateTimeImmutable('2023-01-01 15:45:00 UTC')) : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_timestamp_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::dateTime('dateTime'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'dateTime' => $faker->dateTimeThisYear,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_timestamp_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::dateTime('dateTime'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'dateTime' => $i % 2 === 0 ? $faker->dateTimeThisYear : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_uuid_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::uuid('uuid'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'uuid' => $faker->uuid,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }

    public function test_writing_uuid_nullable_column() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(FlatColumn::uuid('uuid'));

        $faker = Factory::create();

        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'uuid' => $i % 2 === 0 ? $faker->uuid : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertEquals(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertTrue(\file_exists($path));
        \unlink($path);
    }
}
