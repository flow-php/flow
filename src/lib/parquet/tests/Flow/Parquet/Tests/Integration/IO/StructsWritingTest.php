<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

final class StructsWritingTest extends TestCase
{
    public function test_writing_flat_nullable_structure() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
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
                    'struct' => $i % 2 === 0
                        ? [
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
                        ]
                        : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_flat_structure() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
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
    }

    public function test_writing_flat_structure_with_nullable_elements() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
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
                        'boolean' => $i % 5 === 0 ? $faker->boolean : null,
                        'string' => $i % 10 === 0 ? $faker->text(150) : null,
                        'int32' => $i % 4 === 0 ? $faker->numberBetween(0, Consts::PHP_INT32_MAX) : null,
                        'list_of_int' => $i % 2 === 0
                            ? \array_map(
                                static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                                \range(1, \random_int(2, 10))
                            )
                            : null,
                        'list_of_string' => $i % 2 === 0
                            ? \array_map(
                                static fn ($i) => $faker->text(10),
                                \range(1, \random_int(2, 10))
                            )
                            : null,
                    ],
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }
}
