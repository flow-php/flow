<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_float, type_integer, type_list, type_map, type_string};
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MapTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [['a' => 'a', 'b' => 'b']];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [[1 => 'a', 2 => 'b']];
        yield [[0 => 'a', 1 => 'b']];
        yield [[100 => 'a', 99 => 'b']];
    }

    public function test_casting_map_of_ints_into_map_of_floats() : void
    {
        self::assertSame(
            [
                'a' => 1.0,
                'b' => 2.0,
                'c' => 3.0,
            ],
            type_map(type_string(), type_float())->cast(['a' => 1, 'b' => 2, 'c' => 3])
        );
    }

    public function test_casting_map_of_string_to_ints_into_map_of_int_to_float() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "array" into "map<integer, float>"');

        self::assertSame(
            [
                'a' => 1.0,
                'b' => 2.0,
                'c' => 3.0,
            ],
            type_map(type_integer(), type_float())->cast(['a' => 1, 'b' => 2, 'c' => 3])
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_map(type_integer(), type_string())->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsArray((type_map(type_integer(), type_string()))->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'map<string, string>',
            (type_map(type_string(), type_string()))->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            (type_map(type_string(), type_string()))->isValid(['one' => 'two'])
        );
        self::assertTrue(
            (type_map(type_integer(), type_list(type_integer())))->isValid([[1, 2], [3, 4]])
        );
        self::assertTrue(
            (type_map(type_integer(), type_map(type_string(), type_list(type_integer()))))
                ->isValid([0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]])
        );
        self::assertFalse(
            (type_map(type_integer(), type_string()))->isValid(['one' => 'two'])
        );
        self::assertFalse(
            (type_map(type_integer(), type_string()))->isValid([1, 2])
        );
        self::assertFalse(
            (type_map(type_string(), type_string()))->isValid(123)
        );
    }
}
