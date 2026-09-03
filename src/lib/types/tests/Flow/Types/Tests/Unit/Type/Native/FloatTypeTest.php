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

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;

final class FloatTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
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

        yield 'numeric string' => [
            'value' => '12.9',
            'expected' => 12.9,
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

        yield 'bool false' => [
            'value' => false,
            'expected' => 0.0,
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

        yield 'DateTimeInterface' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => 1609459200000000.0,
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new DateInterval('P1D'),
            'expected' => 86400000000.0,
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new DOMElement('element', '1.1'),
            'expected' => 1.1,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider(): Generator
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_float()->assert($value);
        } else {
            static::assertIsFloat(type_float()->assert($value));
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
            type_float()->cast($value);
        } else {
            static::assertSame($expected, type_float()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_float()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_float();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('float', type_float()->toString());
    }
}
