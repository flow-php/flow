<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_boolean, type_float, type_integer, type_list, type_map, type_string};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ListTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [['a', 'b']];
        yield [[1 => 'a', 2 => 'b']];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [['a', 'b']];
        yield [[0 => 'a', 1 => 'b']];
    }

    public function test_casting_list_of_ints_to_list_of_floats() : void
    {
        self::assertSame(
            [1.0, 2.0, 3.0],
            type_list(type_float())->cast([1, 2, 3])
        );
    }

    public function test_casting_string_to_list_of_ints() : void
    {
        self::assertSame(
            [1],
            type_list(type_integer())->cast(['1'])
        );
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_list(type_integer())->assert($value);
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsArray(type_list(type_string())->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'list<boolean>',
            (type_list(type_boolean()))->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue((type_list(type_boolean()))->isValid([true, false]));
        self::assertTrue((type_list(type_string()))->isValid(['one', 'two']));
        self::assertTrue((type_list(type_list(type_string())))->isValid([['one', 'two']]));
        self::assertTrue((type_list(type_map(type_string(), type_list(type_integer()))))->isValid([['one' => [1, 2], 'two' => [3, 4]], ['one' => [5, 6], 'two' => [7, 8]]]));
        self::assertFalse((type_list(type_string()))->isValid(['one' => 'two']));
        self::assertFalse((type_list(type_string()))->isValid([1, 2]));
        self::assertFalse((type_list(type_string()))->isValid(123));
        self::assertTrue((type_list(type_integer()))->isValid([]));
    }
}
