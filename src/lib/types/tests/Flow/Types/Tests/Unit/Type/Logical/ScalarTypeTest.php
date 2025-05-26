<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_scalar;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ScalarTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid string' => [
            'value' => 'string',
            'exceptionClass' => null,
        ];

        yield 'valid integer' => [
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'valid float' => [
            'value' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'valid true' => [
            'value' => true,
            'exceptionClass' => null,
        ];

        yield 'valid false' => [
            'value' => false,
            'exceptionClass' => null,
        ];

        yield 'invalid null' => [
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => 'string',
            'exceptionClass' => null,
        ];

        yield 'integer' => [
            'value' => 1,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1.0,
            'expected' => 1.0,
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

        yield 'null' => [
            'value' => null,
            'expected' => '',
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [],
            'expected' => '[]',
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid string' => [
            'value' => 'string',
            'expected' => true,
        ];

        yield 'valid integer' => [
            'value' => 1,
            'expected' => true,
        ];

        yield 'valid float' => [
            'value' => 1.0,
            'expected' => true,
        ];

        yield 'valid true' => [
            'value' => true,
            'expected' => true,
        ];

        yield 'valid false' => [
            'value' => false,
            'expected' => true,
        ];

        yield 'invalid null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [],
            'expected' => false,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_scalar()->assert($value);
        } else {
            self::assertSame($value, type_scalar()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_scalar()->cast($value);
        } else {
            self::assertSame($expected, type_scalar()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_scalar()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_scalar();
        $normalized = $type->normalize();
        $recreated = TypeFactory::fromArray($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'scalar',
            type_scalar()->toString()
        );
    }
}
