<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_from_array;

final class DateTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid DateTimeImmutable' => [
            'value' => new DateTimeImmutable('2024-12-01'),
            'exceptionClass' => null,
        ];

        yield 'valid DateTime' => [
            'value' => new DateTime('2024-12-01'),
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
    }

    public static function cast_data_provider(): Generator
    {
        yield 'string' => [
            'value' => '2021-01-01 00:00:00',
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'int' => [
            'value' => 1609459200,
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1609459200.0,
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'bool' => [
            'value' => true,
            'expected' => new DateTimeImmutable('1970-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'DateTimeInterface' => [
            'value' => new DateTimeImmutable('2021-01-01 15:00:00'),
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new DateInterval('P1D'),
            'expected' => new DateTimeImmutable('1970-01-02 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new DOMElement('element', '2021-01-01 12:32:00'),
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid DateTime with date only' => [
            'value' => new DateTime('2024-12-01'),
            'expected' => true,
        ];

        yield 'invalid DateTimeImmutable with time' => [
            'value' => new DateTimeImmutable(),
            'expected' => false,
        ];

        yield 'invalid date string' => [
            'value' => '2020-01-01',
            'expected' => false,
        ];

        yield 'invalid datetime string' => [
            'value' => '2020-01-01 00:00:00',
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
            type_date()->assert($value);
        } else {
            static::assertInstanceOf(DateTimeInterface::class, type_date()->assert($value));
        }
    }

    #[TestWith([''])]
    #[TestWith(['t'])]
    #[TestWith(['+12'])]
    #[TestWith(['now'])]
    #[TestWith(['yesterday'])]
    #[TestWith(['12.9'])]
    #[TestWith(['2024-01'])]
    #[TestWith(['2024-001'])]
    #[TestWith(['@1609459200'])]
    #[TestWith(['10:00:00'])]
    public function test_a_wall_clock_string_is_refused(string $value): void
    {
        $this->expectException(CastingException::class);

        type_date()->cast($value);
    }

    public function test_a_compact_iso_date_casts(): void
    {
        static::assertSame('2024-03-05', type_date()->cast('20240305')->format('Y-m-d'));
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_date()->cast($value);
        } else {
            static::assertEquals($expected, type_date()->cast($value));
        }
    }

    public function test_date_type_rejects_non_midnight(): void
    {
        static::assertFalse(type_date()->isValid(new DateTimeImmutable('2024-01-02 03:04:05')));
    }

    public function test_date_type_rejects_sub_second_midnight(): void
    {
        static::assertFalse(type_date()->isValid(new DateTimeImmutable('2024-01-02 00:00:00.500000')));
        static::assertTrue(type_date()->isValid(new DateTimeImmutable('2024-01-02 00:00:00')));
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_date()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_date();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('date', type_date()->toString());
    }
}
