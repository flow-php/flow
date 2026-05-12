<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_numeric_string;

final class NumericStringTypeTest extends TestCase
{
    public static function assert_data_provider(): \Generator
    {
        yield 'numeric string' => [
            'value' => '12345',
            'exceptionClass' => null,
        ];

        yield 'non numeric string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'float ' => [
            'value' => 1234.1,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'integer ' => [
            'value' => 1234,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): \Generator
    {
        yield 'integer' => [
            'value' => 1234,
            'expected' => '1234',
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 123.412,
            'expected' => '123.412',
            'exceptionClass' => null,
        ];

        yield 'string' => [
            'value' => 'string',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): \Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => false,
        ];

        yield 'numeric integer string ' => [
            'value' => '1234',
            'expected' => true,
        ];

        yield 'numeric float string ' => [
            'value' => '1234.1',
            'expected' => true,
        ];

        yield 'float ' => [
            'value' => 1234.1,
            'expected' => false,
        ];

        yield 'integer ' => [
            'value' => 1234,
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
            type_numeric_string()->assert($value);
        } else {
            static::assertIsNumeric(type_numeric_string()->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_numeric_string()->cast($value);
        } else {
            static::assertEquals($expected, type_numeric_string()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_numeric_string()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_numeric_string();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('numeric-string', type_numeric_string()->toString());
    }
}
