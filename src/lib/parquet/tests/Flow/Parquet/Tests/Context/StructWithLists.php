<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

use function array_map;
use function Flow\ETL\DSL\generate_random_int;
use function range;

/**
 * A struct of scalars and two lists, with random rows of that shape.
 */
final class StructWithLists
{
    /**
     * @return array<string, mixed>
     */
    public static function row(): array
    {
        $faker = Factory::create();

        return [
            'struct' => [
                'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                'boolean' => $faker->boolean(),
                'string' => $faker->text(150),
                'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                'list_of_int' => array_map(
                    static fn($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    range(1, generate_random_int(2, 10)),
                ),
                'list_of_string' => array_map(static fn($i) => $faker->text(10), range(1, generate_random_int(2, 10))),
            ],
        ];
    }

    public static function schema(): Schema
    {
        return Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));
    }
}
