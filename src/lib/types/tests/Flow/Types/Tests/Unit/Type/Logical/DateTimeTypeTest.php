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
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Logical\DateTimeType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_from_array;

final class DateTimeTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => null,
        ];

        yield 'valid DateTime' => [
            'value' => new DateTime(),
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
            'expected' => new DateTimeImmutable('1970-01-01 00:00:01'),
            'exceptionClass' => null,
        ];

        yield 'DateTimeInterface' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new DateInterval('P1D'),
            'expected' => new DateTimeImmutable('1970-01-02 00:00:00'),
            'exceptionClass' => null,
        ];

        yield 'DOMElement' => [
            'value' => new DOMElement('element', '2021-01-01 00:00:00'),
            'expected' => new DateTimeImmutable('2021-01-01 00:00:00'),
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'expected' => true,
        ];

        yield 'valid DateTime' => [
            'value' => new DateTime(),
            'expected' => true,
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

    public static function provide_non_iso_inputs(): Generator
    {
        yield 'iso date' => ['2024-01-01', '2024-01-01 00:00:00 Europe/Warsaw'];
        yield 'free-form date' => ['02-Jun-2022', '2022-06-02 00:00:00 Europe/Warsaw'];
        yield 'timestamp' => [1609459200, '2021-01-01 01:00:00 Europe/Warsaw'];
        yield 'bool' => [true, '1970-01-01 01:00:01 Europe/Warsaw'];
        yield 'interval' => [new DateInterval('P1D'), '1970-01-02 01:00:00 Europe/Warsaw'];
        yield 'mutable datetime' => [
            new DateTime('2021-01-01 00:00:00', new DateTimeZone('UTC')),
            '2021-01-01 01:00:00 Europe/Warsaw',
        ];
    }

    public static function provide_iso_date_times(): Generator
    {
        yield ['2024-03-05 12:34:56'];
        yield ['2024-03-05T12:34:56'];
        yield ['2024-03-05 12:34'];
        yield ['2024-03-05 12:34:56.123456'];
        yield ['2024-03-05 12:34:56.123456789'];
        yield ['2024-03-05T12:34:56Z'];
        yield ['2024-03-05 12:34:56+02:00'];
        yield ['2024-03-05 12:34:56-0530'];
        yield ['2024-03-05 12:34:56+02'];
        yield ['2024-02-29 00:00:00'];
        yield ['2024-03-05 24:00:00'];
        yield ['2026-01-02T03:04:05Z'];
        yield ['2026-01-02 03:04:05Z'];
        yield ['2026-01-02T03:04Z'];
        yield ["2026-01-02T03:04:05Z\n"];
        yield ['2026-01-02T03:04:05.1Z'];
        yield ['2026-01-02T03:04:05.123456Z'];
        yield ['2026-01-02T03:04:05.123456789Z'];
        yield ['2026-12-31T23:59:60Z'];
        yield ['2026-01-02T24:00:00Z'];
        yield ['0001-01-01T00:00:00Z'];
        yield ['2026-01-02T03:04:05.123456789+02:00'];
        yield ['2026-01-02T03:04:05+00:00'];
        yield ['2026-01-02T03:04:05-0530'];
        yield ['2026-01-02T03:04:05-05'];
        yield ['2026-01-02 03:04'];
        yield ['2026-01-02T03:04:05'];
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_datetime()->assert($value);
        } else {
            static::assertInstanceOf(DateTimeInterface::class, type_datetime()->assert($value));
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

        type_datetime()->cast($value);
    }

    #[TestWith(['2024-01-01'])]
    #[TestWith(['02-Jun-2022'])]
    public function test_a_date_only_string_lands_at_midnight(string $value): void
    {
        static::assertSame('00:00:00', type_datetime()->cast($value)->format('H:i:s'));
    }

    public function test_a_compact_iso_date_casts(): void
    {
        static::assertSame('2024-03-05', type_datetime()->cast('20240305')->format('Y-m-d'));
    }

    #[DataProvider('provide_iso_date_times')]
    public function test_an_iso_date_time_casts_to_the_object_the_constructor_builds(string $value): void
    {
        static::assertSame(
            serialize((new DateTimeImmutable($value, new DateTimeZone('UTC')))->setTimezone(new DateTimeZone('UTC'))),
            serialize(type_datetime()->cast($value)),
        );
    }

    #[DataProvider('provide_iso_date_times')]
    public function test_cast_into_a_zoned_column(string $value): void
    {
        foreach (['Europe/Warsaw', '+05:30'] as $name) {
            $zone = new DateTimeZone($name);

            static::assertSame(
                serialize((new DateTimeImmutable($value, $zone))->setTimezone($zone)),
                serialize(type_datetime($name)->cast($value)),
            );
        }
    }

    #[TestWith(['2023-02-29 10:00:00'])]
    #[TestWith(['2024-04-31T10:00:00Z'])]
    #[TestWith(['2024-13-01 10:00:00'])]
    #[TestWith(['2024-03-05 25:00:00'])]
    #[TestWith(['2024-03-05 12:60:00'])]
    #[TestWith(['2026-01-02T25:99:99Z'])]
    #[TestWith(['2026-01-02T03:60:00Z'])]
    #[TestWith(['2026-01-02T03:04:61Z'])]
    public function test_an_iso_date_time_off_the_calendar_or_clock_is_refused(string $value): void
    {
        $this->expectException(CastingException::class);

        type_datetime()->cast($value);
    }

    #[TestWith(['2026-01-02'])]
    #[TestWith(['2024-02-29'])]
    #[TestWith(["2026-01-02\n"])]
    #[TestWith(['0001-01-01'])]
    #[TestWith(['9999-12-31'])]
    public function test_an_iso_date_casts_to_the_object_the_constructor_builds(string $value): void
    {
        static::assertSame(serialize(new DateTimeImmutable($value)), serialize(type_datetime()->cast($value)));
    }

    #[TestWith(['2026-02-30'])]
    #[TestWith(['2026-13-01'])]
    #[TestWith(['0000-01-01'])]
    public function test_an_iso_date_off_the_calendar_is_refused(string $value): void
    {
        $this->expectException(CastingException::class);

        type_datetime()->cast($value);
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_datetime()->cast($value);
        } else {
            static::assertEquals($expected, type_datetime()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_datetime()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_datetime();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('datetime', type_datetime()->toString());
    }

    public function test_abbreviation_is_refused_with_its_region(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Time zone "CET" is parsed by PHP as a fixed-offset abbreviation without daylight-saving rules, use the region it links to: "Europe/Brussels"',
        );

        type_datetime('CET');
    }

    public function test_date_widened_into_a_western_zone_shows_the_previous_evening(): void
    {
        static::assertSame(
            '2026-01-01 19:00 America/New_York',
            type_datetime('America/New_York')->cast(type_date()->cast('2026-01-02'))->format('Y-m-d H:i e'),
        );
    }

    public function test_dst_gap_moves_forward(): void
    {
        static::assertSame(
            '2026-03-29 03:30:00 +02:00',
            type_datetime('Europe/Warsaw')->cast('2026-03-29 02:30:00')->format('Y-m-d H:i:s P'),
        );
        static::assertSame(
            '2026-03-08 03:30:00 -04:00',
            type_datetime('America/New_York')->cast('2026-03-08 02:30:00')->format('Y-m-d H:i:s P'),
        );
    }

    public function test_dst_overlap_follows_timelib(): void
    {
        static::assertSame(
            '2026-10-25 02:30:00 +01:00',
            type_datetime('Europe/Warsaw')->cast('2026-10-25 02:30:00')->format('Y-m-d H:i:s P'),
        );
        static::assertSame(
            '2026-11-01 01:30:00 -04:00',
            type_datetime('America/New_York')->cast('2026-11-01 01:30:00')->format('Y-m-d H:i:s P'),
        );
    }

    #[TestWith(['europe/warsaw', 'Europe/Warsaw'])]
    #[TestWith(['EUROPE/WARSAW', 'Europe/Warsaw'])]
    #[TestWith(['etc/gmt-2', 'Etc/GMT-2'])]
    #[TestWith(['us/eastern', 'US/Eastern'])]
    public function test_iana_names_fold_case(string $zone, string $canonical): void
    {
        static::assertSame($canonical, type_datetime($zone)->zoneName());
    }

    public function test_loose_comparison_of_different_zone_kinds_is_false(): void
    {
        static::assertFalse(type_datetime('+05:30') == type_datetime());
    }

    public function test_naive_string_ignores_default_time_zone(): void
    {
        $previous = date_default_timezone_get();
        date_default_timezone_set('Asia/Tokyo');

        try {
            static::assertSame(
                '2026-01-02 03:04:00 UTC',
                type_datetime()->cast('2026-01-02 03:04')->format('Y-m-d H:i:s e'),
            );
        } finally {
            date_default_timezone_set($previous);
        }
    }

    public function test_normalize_carries_the_zone(): void
    {
        static::assertSame(
            ['type' => 'datetime', 'zone' => 'Europe/Warsaw'],
            type_datetime('Europe/Warsaw')->normalize(),
        );
        static::assertSame('UTC', DateTimeType::fromArray(['type' => 'datetime'])->zoneName());
    }

    public function test_object_in_zone_is_returned_as_is(): void
    {
        $value = new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('Europe/Warsaw'));

        static::assertSame($value, type_datetime('Europe/Warsaw')->cast($value));
    }

    #[TestWith(['+0200', '+02:00'])]
    #[TestWith(['+02', '+02:00'])]
    #[TestWith(['-05:30', '-05:30'])]
    public function test_offsets_canonicalise(string $zone, string $canonical): void
    {
        static::assertSame($canonical, type_datetime($zone)->zoneName());
    }

    public function test_to_string_is_bare_for_utc(): void
    {
        static::assertSame('datetime', type_datetime()->toString());
        static::assertSame('datetime<Europe/Warsaw>', type_datetime('Europe/Warsaw')->toString());
    }

    public function test_types_of_different_zones_are_not_equal(): void
    {
        static::assertFalse(type_equals(type_datetime(), type_datetime('+02:00')));
    }

    #[TestWith(['CEST'])]
    #[TestWith(['PST'])]
    #[TestWith(['+24:00'])]
    #[TestWith(['+02:60'])]
    #[TestWith(['UTC '])]
    #[TestWith([''])]
    #[TestWith(['Mars/Olympus'])]
    public function test_unknown_zone_is_refused(string $zone): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            'Time zone "%s" cannot be a datetime column zone, use an IANA name like "Europe/Warsaw", "UTC" or an offset "+HH:MM"',
            $zone,
        ));

        type_datetime($zone);
    }

    #[TestWith(['utc'])]
    #[TestWith(['z'])]
    #[TestWith(['gmt'])]
    #[TestWith(['gmt0'])]
    #[TestWith(['gmt+0'])]
    #[TestWith(['gmt-0'])]
    #[TestWith(['uct'])]
    #[TestWith(['universal'])]
    #[TestWith(['zulu'])]
    #[TestWith(['greenwich'])]
    #[TestWith(['etc/utc'])]
    #[TestWith(['etc/uct'])]
    #[TestWith(['etc/gmt'])]
    #[TestWith(['etc/gmt0'])]
    #[TestWith(['etc/gmt+0'])]
    #[TestWith(['etc/gmt-0'])]
    #[TestWith(['etc/universal'])]
    #[TestWith(['etc/zulu'])]
    #[TestWith(['etc/greenwich'])]
    #[TestWith(['Z'])]
    #[TestWith(['+00:00'])]
    #[TestWith(['-00:00'])]
    #[TestWith(['+0000'])]
    public function test_zone_aliases_canonicalise_to_utc(string $zone): void
    {
        static::assertSame('UTC', type_datetime($zone)->zoneName());
    }

    public function test_zone_object_alias_canonicalises_to_utc(): void
    {
        static::assertSame('UTC', type_datetime(new DateTimeZone('Z'))->zoneName());
    }

    #[DataProvider('provide_non_iso_inputs')]
    public function test_non_iso_inputs_cast_into_a_zoned_column(mixed $value, string $expected): void
    {
        static::assertSame($expected, type_datetime('Europe/Warsaw')->cast($value)->format('Y-m-d H:i:s e'));
    }

    public function test_cast_does_not_mutate_a_datetime(): void
    {
        $value = new DateTime('2021-01-01 00:00:00', new DateTimeZone('UTC'));

        type_datetime('Europe/Warsaw')->cast($value);

        static::assertSame('2021-01-01 00:00:00 UTC', $value->format('Y-m-d H:i:s e'));
    }
}
