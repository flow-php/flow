<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use DateTimeImmutable;
use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function extension_loaded;
use function file_exists;
use function floatval;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\generate_random_string;
use function iterator_to_array;
use function json_encode;
use function max;
use function min;
use function mkdir;
use function mt_rand;
use function pack;
use function range;
use function round;
use function sprintf;
use function unlink;
use function unpack;

class SimpleTypesWritingTest extends ParquetIntegrationTestCase
{
    /**
     * 100 rows cycling values that are NOT exactly representable in binary32, paired with what
     * reading them back must produce. `10.25` is the exactly-representable control.
     *
     * @param callable(int, float): ?float $pick
     *
     * @return array{0: list<array{float: null|float}>, 1: list<array{float: null|float}>}
     */
    public static function float32Cases(callable $pick): array
    {
        $values = [10.25, 0.1, 1 / 3, 1.0e-8, 18.52, -0.1];
        $input = [];
        $widened = [];

        foreach (range(1, 100) as $i) {
            $value = $pick($i, $values[$i % count($values)]);
            $input[] = ['float' => $value];
            $widened[] = ['float' => $value === null ? null : unpack('g', pack('g', $value))[1]];
        }

        return [$input, $widened];
    }

    public static function decimalPrecisionProvider(): array
    {
        $precisions = [
            'precision 4, scale 2' => [4, 2, 99.99],
            'precision 6, scale 2' => [6, 2, 9999.99],
            'precision 8, scale 2' => [8, 2, 999999.99],
            'precision 10, scale 2' => [10, 2, 99999999.99],
            'precision 10, scale 4' => [10, 4, 999999.9999],
            'precision 15, scale 2' => [15, 2, 9999999999999.99],
            'precision 18, scale 6' => [18, 6, 999999999999.999999],
            'precision 5, scale 0' => [5, 0, 99999.0],
            'precision 8, scale 8' => [8, 8, 0.99999999],
        ];

        $engines = ['php' => new PhpParquetEngine()];

        if (extension_loaded('arrow')) {
            $engines['arrow'] = new ArrowParquetEngine();
        }

        $result = [];

        foreach ($engines as $engineName => $engine) {
            foreach ($precisions as $label => $params) {
                $result["{$engineName} {$label}"] = [$engine, ...$params];
            }
        }

        return $result;
    }

    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    #[DataProvider('engine_provider')]
    public function test_writing_bool_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::boolean('boolean'));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'boolean' => ((bool) $i % 2) == 0,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_bool_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::boolean('boolean'));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'boolean' => ($i % 2) == 0 ? (bool) generate_random_int(0, 1) : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_date_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::date('date'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'date' => DateTimeImmutable::createFromMutable($faker->dateTimeThisYear())->setTime(0, 0, 0, 0),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_date_column_before_1970(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::date('date'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'date' => DateTimeImmutable::createFromMutable($faker->dateTimeBetween(
                        '1930-01-01',
                        '1969-01-01',
                    ))->setTime(0, 0, 0, 0),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_date_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::date('date'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'date' => ($i % 2) === 0
                        ? DateTimeImmutable::createFromMutable($faker->dateTimeThisYear())->setTime(0, 0, 0, 0)
                        : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_decimal_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::decimal('decimal'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'decimal' => round($faker->randomFloat(2, 0, 99999999.99), 2),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('decimalPrecisionProvider')]
    public function test_writing_decimal_column_with_different_precisions(
        ParquetEngine $engine,
        int $precision,
        int $scale,
        float $maxValue,
    ): void {
        $path = __DIR__ . '/var/test-writer-parquet-decimal-precision-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::decimal('decimal', $precision, $scale));

        $inputData = [];
        $intDigits = max(0, min($precision - $scale, 9));
        $fracDigits = min($scale, 6);
        $intMax = $intDigits > 0 ? (int) 10 ** $intDigits - 1 : 0;
        $fracMax = $fracDigits > 0 ? (int) 10 ** $fracDigits - 1 : 0;

        for ($i = 0; $i < 50; $i++) {
            $intPart = $intMax > 0 ? mt_rand(0, $intMax) : 0;
            $fracPart = $fracMax > 0 ? mt_rand(0, $fracMax) : 0;
            $value = $scale > 0
                ? floatval(sprintf('%d.%0' . $fracDigits . 'd', $intPart, $fracPart))
                : (float) $intPart;
            $inputData[] = ['decimal' => $value];
        }

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_decimal_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::decimal('decimal'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'decimal' => ($i % 2) === 0 ? round($faker->randomFloat(2, 0, 99999999.99), 2) : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_double_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::double('double'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'double' => $faker->randomFloat(),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_double_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::double('double'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'double' => ($i % 2) === 0 ? $faker->randomFloat() : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_enum_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::enum('enum'));

        $enum = ['A', 'B', 'C', 'D'];

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'enum' => $enum[generate_random_int(0, 3)],
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_float_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::float('float'));

        [$inputData, $widened] = self::float32Cases(static fn(int $i, float $value): float => $value);

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $widened,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_float_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::float('float'));

        [$inputData, $widened] = self::float32Cases(static fn(int $i, float $value): ?float => ($i % 2) === 0
            ? $value
            : null);

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $widened,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_int32_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::int32('int32'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_int32_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::int32('int32'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'int32' => ($i % 2) === 0 ? $faker->numberBetween(0, Consts::PHP_INT32_MAX) : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );

        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_int64(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::int64('int64'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_int64_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::int64('int64'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'int64' => ($i % 2) === 0 ? $faker->numberBetween(0, Consts::PHP_INT64_MAX) : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_json_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::json('json'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'json' => json_encode([
                        'street' => $faker->streetName(),
                        'city' => $faker->city(),
                        'country' => $faker->country(),
                        'zip' => $faker->postcode(),
                    ], JSON_THROW_ON_ERROR),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_json_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::json('json'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'json' => ($i % 2) === 0
                        ? json_encode([
                            'street' => $faker->streetName(),
                            'city' => $faker->city(),
                            'country' => $faker->country(),
                            'zip' => $faker->postcode(),
                        ], JSON_THROW_ON_ERROR)
                        : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_string_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::string('string'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'string' => $faker->text(50),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_string_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::string('string'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'string' => ($i % 2) === 0 ? $faker->text(50) : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_time_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::time('time'));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'time' => (new DateTimeImmutable('2023-01-01 00:00:00 UTC'))->diff(
                        new DateTimeImmutable('2023-01-01 15:45:00 UTC'),
                    ),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_time_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::time('time'));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'time' => ($i % 2) === 0
                        ? (new DateTimeImmutable('2023-01-01 00:00:00 UTC'))->diff(
                            new DateTimeImmutable('2023-01-01 15:45:00 UTC'),
                        )
                        : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_timestamp_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::dateTime('dateTime'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'dateTime' => $faker->dateTimeThisYear(),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_timestamp_column_for_years_before_1970(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::dateTime('dateTime'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'dateTime' => $faker->dateTimeBetween('1930-01-01', '1969-01-01'),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_timestamp_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::dateTime('dateTime'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'dateTime' => ($i % 2) === 0 ? $faker->dateTimeThisYear() : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_uuid_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::uuid('uuid'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'uuid' => $faker->uuid(),
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_uuid_nullable_column(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(FlatColumn::uuid('uuid'));

        $faker = Factory::create();

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'uuid' => ($i % 2) === 0 ? $faker->uuid() : null,
                ],
            ],
            range(1, 100),
        ));

        $writer->write($path, $schema, $inputData);

        static::assertEquals(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertTrue(file_exists($path));
        unlink($path);
    }
}
