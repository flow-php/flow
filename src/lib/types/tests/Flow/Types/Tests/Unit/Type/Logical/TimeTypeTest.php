<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_time;

final class TimeTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid DateInterval' => [
            'value' => new DateInterval('PT10S'),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
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

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'string to time' => [
            'value' => 'PT1S',
            'expected' => new DateInterval('PT1S'),
            'exceptionClass' => null,
        ];

        yield 'datetime to time' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:01'),
            'expected' => new DateInterval('PT1S'),
            'exceptionClass' => null,
        ];

        yield 'date to time' => [
            'value' => new DateTimeImmutable('2021-01-01'),
            'expected' => new DateInterval('PT0S'),
            'exceptionClass' => null,
        ];

        yield 'time stays as is' => [
            'value' => new DateInterval('PT10S'),
            'expected' => new DateInterval('PT10S'),
            'exceptionClass' => null,
        ];

        yield 'clock time to time' => [
            'value' => '12:34:56',
            'expected' => new DateInterval('PT12H34M56S'),
            'exceptionClass' => null,
        ];

        yield 'zero clock time to time' => [
            'value' => '00:00:00',
            'expected' => new DateInterval('PT0H0M0S'),
            'exceptionClass' => null,
        ];

        // PostgreSQL's time is not clamped to a 24h clock; 99:59:59 is a legal value.
        yield 'clock time beyond a day to time' => [
            'value' => '99:59:59',
            'expected' => new DateInterval('PT99H59M59S'),
            'exceptionClass' => null,
        ];

        $fractional = new DateInterval('PT12H34M56S');
        // @mago-ignore analysis:invalid-property-write
        $fractional->f = 0.123456;

        yield 'fractional clock time to time' => [
            'value' => '12:34:56.123456',
            'expected' => $fractional,
            'exceptionClass' => null,
        ];

        $tenth = new DateInterval('PT12H34M56S');
        // @mago-ignore analysis:invalid-property-write
        $tenth->f = 0.5;

        yield 'a one digit fraction is not scaled' => [
            'value' => '12:34:56.5',
            'expected' => $tenth,
            'exceptionClass' => null,
        ];

        yield 'clock time with an out of range minute' => [
            'value' => '25:99:99',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'not a time at all' => [
            'value' => 'not a time',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid DateInterval' => [
            'value' => new DateInterval('PT10S'),
            'expected' => true,
        ];

        yield 'invalid time string' => [
            'value' => '00:00:01',
            'expected' => false,
        ];

        yield 'invalid interval string' => [
            'value' => 'PT10S',
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
            type_time()->assert($value);
        } else {
            static::assertInstanceOf(DateInterval::class, type_time()->assert($value));
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
            type_time()->cast($value);
        } else {
            static::assertEquals($expected, type_time()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_time()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_time();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_time_type_cast_rejects_relative_interval(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Relative DateInterval (with months/years) can't be cast to time");

        type_time()->cast(new DateInterval('P1M'));
    }

    public function test_time_type_rejects_relative_interval(): void
    {
        static::assertFalse(type_time()->isValid(new DateInterval('P1M')));
        static::assertFalse(type_time()->isValid(new DateInterval('P1Y')));
        static::assertTrue(type_time()->isValid(new DateInterval('PT1H2M3S')));
    }

    public function test_to_string(): void
    {
        static::assertSame('time', type_time()->toString());
    }
}
