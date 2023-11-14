<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\ETL\Test\FilesystemTestHelper;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

final class ListsWritingTest extends TestCase
{
    use FilesystemTestHelper;

    public function test_writing_list_of_ints() : void
    {
        $path = $this->createTemporaryFile('parquet-test-', '.parquet');

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'list_of_ints' => \array_map(static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, \random_int(2, 10))),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->removeFile($path);
    }

    public function test_writing_list_of_strings() : void
    {
        $path = $this->createTemporaryFile('parquet-test-', '.parquet');

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_strings', ListElement::string()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'list_of_strings' => \array_map(static fn ($i) => $faker->text(10), \range(1, \random_int(2, 10))),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->removeFile($path);
    }

    public function test_writing_list_with_nullable_elements() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'list_of_ints' => $i % 2 === 0
                        ? \array_map(static fn ($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, \random_int(2, 10)))
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

    public function test_writing_list_with_nullable_list_values() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'list_of_ints' => $i % 2 === 0
                        ? \array_map(static fn ($a) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, \random_int(2, 2)))
                        : [null, null],
                ],
            ];
        }, \range(1, 10)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_nullable_list_of_ints() : void
    {
        $path = $this->createTemporaryFile('parquet-test-', '.parquet');

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::list('list_of_ints', ListElement::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'list_of_ints' => $i % 2 === 0
                        ? \array_map(static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX), \range(1, \random_int(2, 10)))
                        : null,
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->removeFile($path);
    }
}
