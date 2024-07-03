<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{MapKey, MapValue, NestedColumn};
use Flow\Parquet\{Consts, Reader, Writer};
use PHPUnit\Framework\TestCase;

final class MapsWritingTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_writing_map_of_int_int() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_map_of_int_string() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

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

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }

    public function test_writing_nullable_map_of_int_int() : void
    {
        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $writer = new Writer();
        $schema = Schema::with(NestedColumn::map('map_int_int', MapKey::int32(), MapValue::int32()));

        $faker = Factory::create();
        $inputData = \array_merge(...\array_map(static function (int $i) use ($faker) : array {
            return [
                [
                    'map_int_int' => $i % 2 === 0
                        ? \array_merge(
                            ...\array_map(
                                static fn ($i) => [$i => $faker->numberBetween(0, Consts::PHP_INT32_MAX)],
                                \range(1, \random_int(2, 10))
                            )
                        )
                        : null,
                ],
            ];
        }, \range(0, 99)));

        $writer->write($path, $schema, $inputData);

        self::assertSame(
            $inputData,
            \iterator_to_array((new Reader())->read($path)->values())
        );
    }
}
