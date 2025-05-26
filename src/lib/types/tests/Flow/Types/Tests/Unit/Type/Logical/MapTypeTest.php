<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_float, type_from_array, type_integer, type_list, type_map, type_string};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MapTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid map with integer keys' => [
            'value' => [1 => 'a', 2 => 'b'],
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => null,
        ];

        yield 'valid map with sequential keys' => [
            'value' => [0 => 'a', 1 => 'b'],
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => null,
        ];

        yield 'valid map with non-sequential integer keys' => [
            'value' => [100 => 'a', 99 => 'b'],
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid map with string keys for integer key type' => [
            'value' => ['a' => 'a', 'b' => 'b'],
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'mapType' => type_map(type_integer(), type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'map of ints to map of floats' => [
            'value' => ['a' => 1, 'b' => 2, 'c' => 3],
            'mapType' => type_map(type_string(), type_float()),
            'expected' => ['a' => 1.0, 'b' => 2.0, 'c' => 3.0],
            'exceptionClass' => null,
        ];

        yield 'map of string to ints into map of int to float' => [
            'value' => ['a' => 1, 'b' => 2, 'c' => 3],
            'mapType' => type_map(type_integer(), type_float()),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid map with string keys and string values' => [
            'value' => ['one' => 'two'],
            'mapType' => type_map(type_string(), type_string()),
            'expected' => true,
        ];

        yield 'valid map with integer keys and list values' => [
            'value' => [[1, 2], [3, 4]],
            'mapType' => type_map(type_integer(), type_list(type_integer())),
            'expected' => true,
        ];

        yield 'valid complex nested map' => [
            'value' => [0 => ['one' => [1, 2]], 1 => ['two' => [3, 4]]],
            'mapType' => type_map(type_integer(), type_map(type_string(), type_list(type_integer()))),
            'expected' => true,
        ];

        yield 'invalid map with string keys for integer key type' => [
            'value' => ['one' => 'two'],
            'mapType' => type_map(type_integer(), type_string()),
            'expected' => false,
        ];

        yield 'invalid indexed array for map' => [
            'value' => [1, 2],
            'mapType' => type_map(type_integer(), type_string()),
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'mapType' => type_map(type_string(), type_string()),
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, $mapType, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $mapType->assert($value);
        } else {
            self::assertIsArray($mapType->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, $mapType, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $mapType->cast($value);
        } else {
            self::assertSame($expected, $mapType->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, $mapType, bool $expected) : void
    {
        self::assertSame($expected, $mapType->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_map(type_string(), type_integer());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'map<string, string>',
            (type_map(type_string(), type_string()))->toString()
        );
    }
}
