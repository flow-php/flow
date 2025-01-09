<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\{generate_random_int, generate_random_string};
use Faker\Factory;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\{Consts, Reader, Writer};
use PHPUnit\Framework\TestCase;

final class ListsWritingTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_writing_empty_lists_of_ints() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => [],
            ],
        ], \range(1, 1)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_list_of_ints() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => \array_map(static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, generate_random_int(2, 10))),
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_list_of_strings() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_strings', ListElement::string()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_strings' => \array_map(static fn ($i) => $faker->text(10), \range(1, generate_random_int(2, 10))),
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_list_of_structures() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(
            NestedColumn::list('list_of_structs', ListElement::structure(
                [
                    FlatColumn::int32('id'),
                    FlatColumn::string('name'),
                ]
            ))
        );

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_structs' => \array_map(static fn ($i) => [
                    'id' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'name' => $faker->text(10),
                ], \range(1, generate_random_int(2, 10))),
            ],
        ], \range(1, 10)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_list_with_nullable_elements() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => $i % 2 === 0
                    ? \array_map(static fn ($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, generate_random_int(2, 10)))
                    : null,
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_list_with_nullable_list_values() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => $i % 2 === 0
                    ? \array_map(static fn ($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, generate_random_int(2, 2)))
                    : [null, null],
            ],
        ], \range(1, 10)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_nullable_list_of_ints() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => $i % 2 === 0
                    ? \array_map(static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, generate_random_int(2, 10)))
                    : null,
            ],
        ], \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_nullable_list_of_structures() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(
            NestedColumn::list(
                'list_of_structs',
                ListElement::structure(
                    [
                        FlatColumn::int32('id'),
                        FlatColumn::string('name'),
                    ],
                )
            )
        );

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_structs' => $i % 2 === 0
                    ? \array_map(static fn ($i) => [
                        'id' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                        'name' => $faker->text(10),
                    ], \range(1, generate_random_int(2, 10)))
                    : null,
            ],
        ], \range(1, 10)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_nullable_lists_of_ints() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . generate_random_string() . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $inputData = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'list_of_ints' => null,
            ],
        ], \range(1, 10)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }
}
