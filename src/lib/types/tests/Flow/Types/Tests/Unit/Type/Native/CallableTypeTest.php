<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_from_array;

final class CallableTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid callable function name' => [
            'value' => 'printf',
            'exceptionClass' => null,
        ];

        yield 'valid callable function' => [
            'value' => 'count',
            'exceptionClass' => null,
        ];

        yield 'valid callable closure' => [
            'value' => static function (): void {},
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'some_string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'callable function name' => [
            'value' => 'printf',
            'expected' => 'printf',
            'exceptionClass' => null,
        ];

        yield 'callable closure' => [
            'value' => $closure = static function (): void {},
            'expected' => $closure,
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'non_existent_function',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid callable function name' => [
            'value' => 'printf',
            'expected' => true,
        ];

        yield 'valid callable closure' => [
            'value' => static function (): void {},
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

        yield 'invalid integer' => [
            'value' => 123,
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
            type_callable()->assert($value);
        } else {
            static::assertIsCallable(type_callable()->assert($value));
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
            type_callable()->cast($value);
        } else {
            static::assertSame($expected, type_callable()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_callable()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_callable();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('callable', type_callable()->toString());
    }
}
