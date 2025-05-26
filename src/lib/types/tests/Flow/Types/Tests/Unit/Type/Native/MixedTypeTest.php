<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_from_array, type_mixed};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MixedTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'null' => [
            'value' => null,
            'exceptionClass' => null,
        ];

        yield 'true' => [
            'value' => true,
            'exceptionClass' => null,
        ];

        yield 'false' => [
            'value' => false,
            'exceptionClass' => null,
        ];

        yield 'integer 0' => [
            'value' => 0,
            'exceptionClass' => null,
        ];

        yield 'integer 1' => [
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'float 0.0' => [
            'value' => 0.0,
            'exceptionClass' => null,
        ];

        yield 'float 1.0' => [
            'value' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'string' => [
            'value' => 'string',
            'exceptionClass' => null,
        ];

        yield 'empty array' => [
            'value' => [],
            'exceptionClass' => null,
        ];

        yield 'array with integer' => [
            'value' => [1],
            'exceptionClass' => null,
        ];

        yield 'array with key-value' => [
            'value' => [1 => 2],
            'exceptionClass' => null,
        ];

        yield 'empty object' => [
            'value' => (object) [],
            'exceptionClass' => null,
        ];

        yield 'object with property' => [
            'value' => (object) ['a' => 'b'],
            'exceptionClass' => null,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'null' => [
            'value' => null,
            'expected' => null,
            'exceptionClass' => null,
        ];

        yield 'true' => [
            'value' => true,
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'false' => [
            'value' => false,
            'expected' => false,
            'exceptionClass' => null,
        ];

        yield 'integer 0' => [
            'value' => 0,
            'expected' => 0,
            'exceptionClass' => null,
        ];

        yield 'integer 1' => [
            'value' => 1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'float 0.0' => [
            'value' => 0.0,
            'expected' => 0.0,
            'exceptionClass' => null,
        ];

        yield 'float 1.0' => [
            'value' => 1.0,
            'expected' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'string' => [
            'value' => 'string',
            'expected' => 'string',
            'exceptionClass' => null,
        ];

        yield 'empty array' => [
            'value' => [],
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'array with integer' => [
            'value' => [1],
            'expected' => [1],
            'exceptionClass' => null,
        ];

        yield 'array with key-value' => [
            'value' => [1 => 2],
            'expected' => [1 => 2],
            'exceptionClass' => null,
        ];

        yield 'empty object' => [
            'value' => $obj1 = (object) [],
            'expected' => $obj1,
            'exceptionClass' => null,
        ];

        yield 'object with property' => [
            'value' => $obj2 = (object) ['a' => 'b'],
            'expected' => $obj2,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'null' => [
            'value' => null,
            'expected' => true,
        ];

        yield 'true' => [
            'value' => true,
            'expected' => true,
        ];

        yield 'false' => [
            'value' => false,
            'expected' => true,
        ];

        yield 'integer 0' => [
            'value' => 0,
            'expected' => true,
        ];

        yield 'integer 1' => [
            'value' => 1,
            'expected' => true,
        ];

        yield 'float 0.0' => [
            'value' => 0.0,
            'expected' => true,
        ];

        yield 'float 1.0' => [
            'value' => 1.0,
            'expected' => true,
        ];

        yield 'string' => [
            'value' => 'string',
            'expected' => true,
        ];

        yield 'empty array' => [
            'value' => [],
            'expected' => true,
        ];

        yield 'array with integer' => [
            'value' => [1],
            'expected' => true,
        ];

        yield 'array with key-value' => [
            'value' => [1 => 2],
            'expected' => true,
        ];

        yield 'empty object' => [
            'value' => (object) [],
            'expected' => true,
        ];

        yield 'object with property' => [
            'value' => (object) ['a' => 'b'],
            'expected' => true,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_mixed()->assert($value);
        } else {
            self::assertEquals($value, type_mixed()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_mixed()->cast($value);
        } else {
            self::assertEquals($expected, type_mixed()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_mixed()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_mixed();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertEquals('mixed', type_mixed()->toString());
    }
}
