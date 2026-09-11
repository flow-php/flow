<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\generate_random_int;
use function iterator_to_array;
use function range;

class ListsWritingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_writing_empty_lists_of_ints(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => [],
                ],
            ],
            range(1, 1),
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
    public function test_writing_list_of_decimals(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('prices', ListElement::decimal(18, 2)));

        $inputData = [['prices' => [123.45, 1.23]]];

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
    public function test_writing_list_of_ints(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => array_map(
                        static fn($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        range(1, generate_random_int(2, 10)),
                    ),
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
    public function test_writing_list_of_strings(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_strings', ListElement::string()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_strings' => array_map(
                        static fn($i) => $faker->text(10),
                        range(1, generate_random_int(2, 10)),
                    ),
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
    public function test_writing_list_of_structures(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('id'),
            FlatColumn::string('name'),
        ])));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_structs' => array_map(
                        static fn($i) => [
                            'id' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            'name' => $faker->text(10),
                        ],
                        range(1, generate_random_int(2, 10)),
                    ),
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
    public function test_writing_list_with_nullable_elements(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => ($i % 2) === 0
                        ? array_map(
                            static fn($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            range(1, generate_random_int(2, 10)),
                        )
                        : null,
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
    public function test_writing_list_with_nullable_list_values(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => ($i % 2) === 0
                        ? array_map(
                            static fn($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            range(1, generate_random_int(2, 2)),
                        )
                        : [null, null],
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
    public function test_writing_nullable_list_of_ints(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => ($i % 2) === 0
                        ? array_map(
                            static fn($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                            range(1, generate_random_int(2, 10)),
                        )
                        : null,
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
    public function test_writing_nullable_list_of_structures(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('id'),
            FlatColumn::string('name'),
        ])));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_structs' => ($i % 2) === 0
                        ? array_map(
                            static fn($i) => [
                                'id' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                                'name' => $faker->text(10),
                            ],
                            range(1, generate_random_int(2, 10)),
                        )
                        : null,
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
    public function test_writing_nullable_list_of_structures_with_required_fields(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_structs', ListElement::structure([
            FlatColumn::int32('id', Repetition::REQUIRED),
        ], true)));

        $faker = Factory::create();
        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_structs' => array_map(
                        static fn($i) => [
                            'id' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        ],
                        range(1, generate_random_int(2, 10)),
                    ),
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
    public function test_writing_nullable_lists_of_ints(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        $writer = new Writer(engine: $engine);
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'list_of_ints' => null,
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
}
