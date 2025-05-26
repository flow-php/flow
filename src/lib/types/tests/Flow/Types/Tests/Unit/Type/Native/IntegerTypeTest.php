<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_from_array, type_integer};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntegerTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid integer 1234' => [
            'value' => 1234,
            'exceptionClass' => null,
        ];

        yield 'valid integer PHP_INT_MAX' => [
            'value' => PHP_INT_MAX,
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime' => [
            'value' => new \DateTime(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'int' => [
            'value' => 1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1.1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'bool' => [
            'value' => true,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [1, 2, 3],
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'stdClass' => [
            'value' => new \stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'DateTimeInterface' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => 1609459200000000,
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new \DateInterval('P1D'),
            'expected' => 86400000000,
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new \DOMElement('element', '1'),
            'expected' => 1,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid integer' => [
            'value' => 1,
            'expected' => true,
        ];

        yield 'valid negative integer' => [
            'value' => -5,
            'expected' => true,
        ];

        yield 'invalid string' => [
            'value' => 'one',
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'expected' => false,
        ];

        yield 'invalid float' => [
            'value' => 123.0,
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer()->assert($value);
        } else {
            self::assertIsInt(type_integer()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer()->cast($value);
        } else {
            self::assertSame($expected, type_integer()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_integer()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_integer();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'integer',
            type_integer()->toString()
        );
    }
}
