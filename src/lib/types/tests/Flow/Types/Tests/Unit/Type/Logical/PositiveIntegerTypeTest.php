<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateInterval;
use DateTimeImmutable;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_positive_integer;

final class PositiveIntegerTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid positive integer 1' => [
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'invalid zero' => [
            'value' => 0,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid negative integer' => [
            'value' => -1,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid positive integer PHP_INT_MAX' => [
            'value' => PHP_INT_MAX,
            'exceptionClass' => null,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'valid positive integer 1' => [
            'value' => 1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'invalid zero' => [
            'value' => 0,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid negative integer' => [
            'value' => -1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'valid positive integer PHP_INT_MAX' => [
            'value' => PHP_INT_MAX,
            'expected' => PHP_INT_MAX,
            'exceptionClass' => null,
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
            'expected' => PHP_INT_MAX,
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
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid positive integer 1' => [
            'value' => 1,
            'expected' => true,
        ];

        yield 'invalid zero' => [
            'value' => 0,
            'expected' => false,
        ];

        yield 'invalid negative integer' => [
            'value' => -1,
            'expected' => false,
        ];

        yield 'valid positive integer PHP_INT_MAX' => [
            'value' => PHP_INT_MAX,
            'expected' => true,
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
            type_positive_integer()->assert($value);
        } else {
            $result = type_positive_integer()->assert($value);
            static::assertIsInt($result);
            static::assertSame($value, $result);
        }
    }

    public function test_an_out_of_range_literal_names_the_range_as_the_reason(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('value is out of range for integer');

        type_positive_integer()->cast('9223372036854775808');
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_positive_integer()->cast($value);
        } else {
            static::assertSame($expected, type_positive_integer()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_positive_integer()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_positive_integer();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('positive_integer', type_positive_integer()->toString());
    }
}
