<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;

final class IntegerTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
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
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime' => [
            'value' => new DateTime(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'numeric string keeps its leading zeros' => [
            'value' => '01234',
            'expected' => 1234,
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

        yield 'bool false' => [
            'value' => false,
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [1, 2, 3],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'null' => [
            'value' => null,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'stdClass' => [
            'value' => new stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'DateTimeInterface' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => 1609459200,
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new DateInterval('P1D'),
            'expected' => 86400,
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new DOMElement('element', '1'),
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'DOMElement carrying an out-of-range literal' => [
            'value' => new DOMElement('element', '9223372036854775808'),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'integer literal above the maximum' => [
            'value' => '9223372036854775808',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'integer literal below the minimum' => [
            'value' => '-9223372036854775809',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'float-shaped text above the maximum' => [
            'value' => '9223372036854775807.0',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'scientific text above the maximum' => [
            'value' => '9.2233720368547758e18',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'double out of range' => [
            'value' => 1e300,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'infinity' => [
            'value' => INF,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'not a number' => [
            'value' => NAN,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'the maximum itself still casts' => [
            'value' => '9223372036854775807',
            'expected' => 9223372036854775807,
            'exceptionClass' => null,
        ];

        yield 'the minimum itself still casts' => [
            'value' => '-9223372036854775808',
            'expected' => -9223372036854775807 - 1,
            'exceptionClass' => null,
        ];

        yield 'zero padded text' => [
            'value' => '007',
            'expected' => 7,
            'exceptionClass' => null,
        ];

        yield 'zero padded text with four digits' => [
            'value' => '01234',
            'expected' => 1234,
            'exceptionClass' => null,
        ];

        yield 'fractional text truncates' => [
            'value' => '12.9',
            'expected' => 12,
            'exceptionClass' => null,
        ];

        yield 'scientific text' => [
            'value' => '1e5',
            'expected' => 100000,
            'exceptionClass' => null,
        ];

        yield 'negative zero' => [
            'value' => '-0',
            'expected' => 0,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider(): Generator
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer()->assert($value);
        } else {
            static::assertIsInt(type_integer()->assert($value));
        }
    }

    public function test_an_out_of_range_literal_names_the_range_as_the_reason(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('value is out of range for integer');

        type_integer()->cast('9223372036854775808');
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_integer()->cast($value);
        } else {
            static::assertSame($expected, type_integer()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_integer()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_integer();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('integer', type_integer()->toString());
    }
}
