<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_integer_range};
use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntegerRangeTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid integer in range' => [
            'min' => 1,
            'max' => 10,
            'value' => 5,
            'exceptionClass' => null,
        ];

        yield 'valid integer at min boundary' => [
            'min' => 1,
            'max' => 10,
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'valid integer at max boundary' => [
            'min' => 1,
            'max' => 10,
            'value' => 10,
            'exceptionClass' => null,
        ];

        yield 'invalid integer below range' => [
            'min' => 1,
            'max' => 10,
            'value' => 0,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer above range' => [
            'min' => 1,
            'max' => 10,
            'value' => 11,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string' => [
            'min' => 1,
            'max' => 10,
            'value' => '5',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'min' => 1,
            'max' => 10,
            'value' => 5.5,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null' => [
            'min' => 1,
            'max' => 10,
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'valid integer in range' => [
            'min' => 1,
            'max' => 10,
            'value' => 5,
            'expected' => 5,
            'exceptionClass' => null,
        ];

        yield 'cast string to integer' => [
            'min' => 1,
            'max' => 10,
            'value' => '5',
            'expected' => 5,
            'exceptionClass' => null,
        ];

        yield 'cast float to integer' => [
            'min' => 1,
            'max' => 10,
            'value' => 5.9,
            'expected' => 5,
            'exceptionClass' => null,
        ];

        yield 'cast boolean true to integer' => [
            'min' => 1,
            'max' => 10,
            'value' => true,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'cast boolean false to integer' => [
            'min' => 0,
            'max' => 10,
            'value' => false,
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'cast null to integer' => [
            'min' => 0,
            'max' => 10,
            'value' => null,
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'cast array to integer' => [
            'min' => 0,
            'max' => 10,
            'value' => [1, 2, 3],
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'cast empty array to integer' => [
            'min' => 0,
            'max' => 10,
            'value' => [],
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'cast DOMElement to integer' => [
            'min' => 0,
            'max' => 100,
            'value' => new \DOMElement('test', '42'),
            'expected' => 42,
            'exceptionClass' => null,
        ];

        yield 'cast DateTimeImmutable to integer' => [
            'min' => 0,
            'max' => PHP_INT_MAX,
            'value' => new \DateTimeImmutable('2023-01-01 00:00:00'),
            'expected' => 1672531200000000,
            'exceptionClass' => null,
        ];

        yield 'cast DateInterval to integer' => [
            'min' => 0,
            'max' => PHP_INT_MAX,
            'value' => new \DateInterval('PT1H'),
            'expected' => 3600000000,
            'exceptionClass' => null,
        ];

        yield 'cast object fails' => [
            'min' => 1,
            'max' => 10,
            'value' => new \stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'cast value outside range fails' => [
            'min' => 1,
            'max' => 10,
            'value' => '15',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid integer' => [
            'value' => 5,
            'expected' => true,
        ];

        yield 'invalid string' => [
            'value' => '5',
            'expected' => false,
        ];

        yield 'invalid float' => [
            'value' => 5.5,
            'expected' => false,
        ];

        yield 'invalid null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid boolean' => [
            'value' => true,
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2, 3],
            'expected' => false,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(int $min, int $max, mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer_range($min, $max)->assert($value);
        } else {
            $result = type_integer_range($min, $max)->assert($value);
            self::assertIsInt($result);
            self::assertSame($value, $result);
            self::assertGreaterThanOrEqual($min, $result);
            self::assertLessThanOrEqual($max, $result);
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(int $min, int $max, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer_range($min, $max)->cast($value);
        } else {
            $result = type_integer_range($min, $max)->cast($value);
            self::assertSame($expected, $result);
            self::assertGreaterThanOrEqual($min, $result);
            self::assertLessThanOrEqual($max, $result);
        }
    }

    public function test_constructor_with_invalid_range() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum value cannot be greater than maximum value.');

        type_integer_range(10, 1);
    }

    public function test_from_array() : void
    {
        $data = [
            'type' => 'integer_range',
            'min' => 10,
            'max' => 20,
        ];

        $type = type_integer_range(1, 1)->fromArray($data);

        self::assertSame('integer<10, 20>', $type->toString());
        self::assertSame($data, $type->normalize());
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_integer_range(1, 10)->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_integer_range(5, 15);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
        self::assertSame([
            'type' => 'integer_range',
            'min' => 5,
            'max' => 15,
        ], $normalized);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'integer<1, 10>',
            type_integer_range(1, 10)->toString()
        );

        self::assertSame(
            'integer<-5, 5>',
            type_integer_range(-5, 5)->toString()
        );

        self::assertSame(
            'integer<0, 100>',
            type_integer_range(0, 100)->toString()
        );
        self::assertSame(
            'integer<min, max>',
            type_integer_range(PHP_INT_MIN, PHP_INT_MAX)->toString()
        );
    }
}
