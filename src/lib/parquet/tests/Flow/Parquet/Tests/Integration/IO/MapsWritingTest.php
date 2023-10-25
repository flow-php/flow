<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\TestCase;

final class MapsWritingTest extends TestCase
{
    public function test_writing_map_of_int_int() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'map_int_int' => \array_merge(
                        ...\array_map(
                            static fn ($i) => [$i => $faker->numberBetween(0, Consts::PHP_INT32_MAX)],
                            \range(1, \random_int(2, 10))
                        )
                    ),
                ],
            ];
        }, \range(1, 100)));

        $writer->write($path, $schema, $inputData);

        $this->assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_map_of_int_string() : void
    {
        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::map('map_int_string', MapKey::int32(), MapValue::string()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'map_int_string' => \array_merge(
                        ...\array_map(
                            static fn ($i) => [$i => $faker->text(10)],
                            \range(1, \random_int(2, 10))
                        )
                    ),
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
