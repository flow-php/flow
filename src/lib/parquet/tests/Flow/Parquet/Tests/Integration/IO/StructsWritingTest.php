<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\{generate_random_int, generate_random_string};
use Faker\Factory;
use Flow\Parquet\{Consts, ParquetEngine, Reader, Writer};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\Attributes\DataProvider;

class StructsWritingTest extends ParquetIntegrationTestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    #[DataProvider('engine_provider')]
    public function test_writing_flat_nullable_structure(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
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
                'struct' => $i % 2 === 0
                    ? [
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
                    ]
                    : null,
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_flat_structure(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
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
    }

    #[DataProvider('engine_provider')]
    public function test_writing_flat_structure_with_nullable_elements(ParquetEngine $engine) : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer(engine: $engine);
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
                    'boolean' => $i % 5 === 0 ? $faker->boolean : null,
                    'string' => $i % 10 === 0 ? $faker->text(150) : null,
                    'int32' => $i % 4 === 0 ? $faker->numberBetween(0, Consts::PHP_INT32_MAX) : null,
                    'list_of_int' => $i % 2 === 0
                        ? \array_map(
                            static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            \range(1, generate_random_int(2, 10))
                        )
                        : null,
                    'list_of_string' => $i % 2 === 0
                        ? \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, generate_random_int(2, 10))
                        )
                        : null,
                ],
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        static::assertSame(
            $inputData,
            \iterator_to_array((new Reader(engine: $engine))->read($path)->values())
        );
    }
}
