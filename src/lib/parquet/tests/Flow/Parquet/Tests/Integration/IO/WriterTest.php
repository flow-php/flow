<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

final class WriterTest extends ParquetIntegrationTestCase
{
    public function test_writer() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();

        $schema = Schema::with(
            FlatColumn::boolean('boolean'),
            FlatColumn::int32('int32'),
            FlatColumn::int64('int64'),
            FlatColumn::float('float'),
            FlatColumn::double('double'),
            FlatColumn::decimal('decimal'),
            FlatColumn::string('string'),
//            FlatColumn::date('date'),
//            FlatColumn::dateTime('datetime'),
//            NestedColumn::list('list_of_datetimes', ListElement::dateTime()),
//            NestedColumn::map('map_of_ints', MapKey::string(), MapValue::int32()),
//            NestedColumn::list('list_of_strings', ListElement::string()),
//            NestedColumn::struct('struct_flat', [
//                FlatColumn::int32('id'),
//                FlatColumn::string('name')
//            ]),
        );

        $faker = Factory::create();

        $inputData = \array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'boolean' => $faker->boolean,
                    'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    'int64' => $faker->numberBetween(0, PHP_INT_MAX),
                    'float' => 10.25,
                    'double' => $faker->randomFloat(),
                    'decimal' => \round($faker->randomFloat(5), 2),
                    'string' => $faker->text(50),
                    //                    'date' => \DateTimeImmutable::createFromMutable($faker->dateTime)->setTime(0, 0, 0, 0),
                    //                    'datetime' => \DateTimeImmutable::createFromMutable($faker->dateTime),
                    //                    'list_of_datetimes' => [
                    //                        \DateTimeImmutable::createFromMutable($faker->dateTime),
                    //                        \DateTimeImmutable::createFromMutable($faker->dateTime),
                    //                        \DateTimeImmutable::createFromMutable($faker->dateTime),
                    //                    ],
                    //                    'map_of_ints' => [
                    //                        'a' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    //                        'b' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    //                        'c' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    //                    ],
                    //                    'list_of_strings' => \array_map(static fn (int $i) => $faker->text(50), \range(0, \random_int(1, 10))),
                    //                    'struct_flat' => [
                    //                        'id' => $i,
                    //                        'name' => 'name_' . \str_pad((string) $i, 5, '0', STR_PAD_LEFT)
                    //                    ]
                ],
            ];
        }, \range(1, 5_000));

        $inputData = \array_merge(...$inputData);

        $writer->write($path, $schema, $inputData);

        $reader = new Reader();
        $file = $reader->read($path);

        $this->assertEquals(
            $inputData,
            \iterator_to_array($file->values()),
        );
    }
}
