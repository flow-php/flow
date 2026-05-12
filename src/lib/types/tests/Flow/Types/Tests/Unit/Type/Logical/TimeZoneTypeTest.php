<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_time_zone;

final class TimeZoneTypeTest extends TestCase
{
    public static function assert_data_provider(): \Generator
    {
        yield 'valid DateTimeZone UTC' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeZone America/New_York' => [
            'value' => new \DateTimeZone('America/New_York'),
            'exceptionClass' => null,
        ];

        yield 'valid DateTimeZone Europe/Warsaw' => [
            'value' => new \DateTimeZone('Europe/Warsaw'),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'UTC',
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

        yield 'invalid integer' => [
            'value' => 123,
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
    }

    public static function cast_data_provider(): \Generator
    {
        yield 'DateTimeZone UTC' => [
            'value' => new \DateTimeZone('UTC'),
            'expected' => new \DateTimeZone('UTC'),
            'exceptionClass' => null,
        ];

        yield 'DateTimeZone America/New_York' => [
            'value' => new \DateTimeZone('America/New_York'),
            'expected' => new \DateTimeZone('America/New_York'),
            'exceptionClass' => null,
        ];

        yield 'string UTC' => [
            'value' => 'UTC',
            'expected' => new \DateTimeZone('UTC'),
            'exceptionClass' => null,
        ];

        yield 'string America/New_York' => [
            'value' => 'America/New_York',
            'expected' => new \DateTimeZone('America/New_York'),
            'exceptionClass' => null,
        ];

        yield 'string Europe/Warsaw' => [
            'value' => 'Europe/Warsaw',
            'expected' => new \DateTimeZone('Europe/Warsaw'),
            'exceptionClass' => null,
        ];

        yield 'string +00:00' => [
            'value' => '+00:00',
            'expected' => new \DateTimeZone('+00:00'),
            'exceptionClass' => null,
        ];

        yield 'string +05:30' => [
            'value' => '+05:30',
            'expected' => new \DateTimeZone('+05:30'),
            'exceptionClass' => null,
        ];

        yield 'DateTimeImmutable with timezone' => [
            'value' => new \DateTimeImmutable('now', new \DateTimeZone('America/New_York')),
            'expected' => new \DateTimeZone('America/New_York'),
            'exceptionClass' => null,
        ];

        yield 'DateTime with timezone' => [
            'value' => new \DateTime('now', new \DateTimeZone('Europe/London')),
            'expected' => new \DateTimeZone('Europe/London'),
            'exceptionClass' => null,
        ];

        yield 'DOMElement with timezone string' => [
            'value' => new \DOMElement('timezone', 'UTC'),
            'expected' => new \DateTimeZone('UTC'),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'invalid-timezone',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid array' => [
            'value' => ['UTC'],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): \Generator
    {
        yield 'valid DateTimeZone UTC' => [
            'value' => new \DateTimeZone('UTC'),
            'expected' => true,
        ];

        yield 'valid DateTimeZone America/New_York' => [
            'value' => new \DateTimeZone('America/New_York'),
            'expected' => true,
        ];

        yield 'invalid string UTC' => [
            'value' => 'UTC',
            'expected' => false,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'expected' => false,
        ];

        yield 'invalid DateTime' => [
            'value' => new \DateTime(),
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'expected' => false,
        ];

        yield 'invalid null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [],
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
            type_time_zone()->assert($value);
        } else {
            static::assertInstanceOf(\DateTimeZone::class, type_time_zone()->assert($value));
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
            type_time_zone()->cast($value);
        } else {
            static::assertEquals($expected, type_time_zone()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_time_zone()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_time_zone();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('timezone', type_time_zone()->toString());
    }
}
