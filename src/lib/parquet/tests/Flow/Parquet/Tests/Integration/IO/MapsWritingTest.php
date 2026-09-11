<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function file_exists;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\generate_random_string;
use function iterator_to_array;
use function mkdir;
use function range;

class MapsWritingTest extends ParquetIntegrationTestCase
{
    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    #[DataProvider('engine_provider')]
    public function test_writing_empty_map_of_int_int(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $inputData = array_merge(...array_map(
            static fn(): array => [
                [
                    'map_int_int' => [],
                ],
            ],
            range(1, 10),
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
    }

    #[DataProvider('engine_provider')]
    public function test_writing_map_of_int_int(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'map_int_int' => array_merge(...array_map(
                        static fn($i) => [$i => $faker->numberBetween(0, Consts::PHP_INT32_MAX)],
                        range(1, generate_random_int(2, 10)),
                    )),
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
    }

    #[DataProvider('engine_provider')]
    public function test_writing_map_of_int_int_with_all_maps_null(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $inputData = array_merge(...array_map(
            static fn(): array => [
                [
                    'map_int_int' => null,
                ],
            ],
            range(1, 10),
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
    }

    #[DataProvider('engine_provider')]
    public function test_writing_map_of_int_string(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('map_int_string', MapKey::int32(), MapValue::string()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'map_int_string' => array_merge(...array_map(
                        static fn($i) => [$i => $faker->text(10)],
                        range(1, generate_random_int(2, 10)),
                    )),
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
    }

    #[DataProvider('engine_provider')]
    public function test_writing_map_of_string_decimal(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('rates', MapKey::string(), MapValue::decimal(18, 2)));

        $inputData = [['rates' => ['usd' => 123.45]]];

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_nullable_map_of_int_int(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'map_int_int' => ($i % 2) === 0
                        ? array_merge(...array_map(
                            static fn($i) => [$i => $faker->numberBetween(0, Consts::PHP_INT32_MAX)],
                            range(1, generate_random_int(2, 10)),
                        ))
                        : null,
                ],
            ],
            range(0, 99),
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
    }
}
