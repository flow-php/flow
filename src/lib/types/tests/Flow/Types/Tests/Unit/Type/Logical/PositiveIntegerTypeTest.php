<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_positive_integer};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PositiveIntegerTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
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

    public static function cast_data_provider() : \Generator
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
    }

    public static function is_valid_data_provider() : \Generator
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

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_positive_integer()->assert($value);
        } else {
            $result = type_positive_integer()->assert($value);
            self::assertIsInt($result);
            self::assertSame($value, $result);
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_positive_integer()->cast($value);
        } else {
            self::assertSame($expected, type_positive_integer()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_positive_integer()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_positive_integer();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'positive_integer',
            type_positive_integer()->toString()
        );
    }
}
