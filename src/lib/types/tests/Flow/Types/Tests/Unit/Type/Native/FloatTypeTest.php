<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_float, type_from_array};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid float 1234.52' => [
            'value' => 1234.52,
            'exceptionClass' => null,
        ];

        yield 'valid float -1234.52' => [
            'value' => -1234.52,
            'exceptionClass' => null,
        ];

        yield 'valid float 1.22e-15' => [
            'value' => 1.22e-15,
            'exceptionClass' => null,
        ];

        yield 'valid float -1.22e-15' => [
            'value' => -1.22e-15,
            'exceptionClass' => null,
        ];

        yield 'valid float .25' => [
            'value' => .25,
            'exceptionClass' => null,
        ];

        yield 'valid float -.25' => [
            'value' => -.25,
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

        yield 'invalid integer' => [
            'value' => 123,
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
            'expected' => 0.0,
            'exceptionClass' => null,
        ];

        yield 'int' => [
            'value' => 1,
            'expected' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1.1,
            'expected' => 1.1,
            'exceptionClass' => null,
        ];

        yield 'bool' => [
            'value' => true,
            'expected' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [1, 2, 3],
            'expected' => 1.0,
            'exceptionClass' => null,
        ];

        yield 'DateTimeInterface' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => 1609459200000000.0,
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new \DateInterval('P1D'),
            'expected' => 86400000000.0,
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new \DOMElement('element', '1.1'),
            'expected' => 1.1,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid float' => [
            'value' => 1.0,
            'expected' => true,
        ];

        yield 'valid negative float' => [
            'value' => -1.5,
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

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_float()->assert($value);
        } else {
            self::assertIsFloat(type_float()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_float()->cast($value);
        } else {
            self::assertSame($expected, type_float()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_float()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_float();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'float',
            type_float()->toString()
        );
    }
}
