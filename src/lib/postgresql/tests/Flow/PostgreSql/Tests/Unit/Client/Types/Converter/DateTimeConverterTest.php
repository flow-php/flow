<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\DateTimeConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class DateTimeConverterTest extends TestCase
{
    public function test_datetime_format() : void
    {
        $converter = new DateTimeConverter();
        $date = new \DateTimeImmutable('2024-06-15 09:00:00.000000+02:00');

        $dbValue = $converter->toDatabase($date);
        self::assertSame('2024-06-15 09:00:00.000000+02:00', $dbValue);
    }

    public function test_non_datetime_returns_empty() : void
    {
        $converter = new DateTimeConverter();
        self::assertSame('', $converter->toDatabase(12345));
    }

    public function test_null_handling() : void
    {
        $converter = new DateTimeConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new DateTimeConverter();
        $date = new \DateTimeImmutable('2024-01-15 14:30:45.123456+00:00');

        $dbValue = $converter->toDatabase($date);
        self::assertNotNull($dbValue);
        self::assertSame('2024-01-15 14:30:45.123456+00:00', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::TIMESTAMPTZ);
        self::assertInstanceOf(\DateTimeInterface::class, $phpValue);
        self::assertSame($date->format('Y-m-d H:i:s'), $phpValue->format('Y-m-d H:i:s'));
    }

    public function test_string_passthrough() : void
    {
        $converter = new DateTimeConverter();
        self::assertSame('2024-01-15 14:30:45', $converter->toDatabase('2024-01-15 14:30:45'));
    }

    public function test_supported_types() : void
    {
        $converter = new DateTimeConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::TIMESTAMP, $types);
        self::assertContains(PostgreSqlType::TIMESTAMPTZ, $types);
    }

    public function test_timestamp_type() : void
    {
        $converter = new DateTimeConverter();
        $result = $converter->toPhp('2024-01-15 14:30:45.123', PostgreSqlType::TIMESTAMP);
        self::assertSame('2024-01-15', $result->format('Y-m-d'));
        self::assertSame('14:30:45', $result->format('H:i:s'));
    }

    public function test_to_php_returns_immutable() : void
    {
        $converter = new DateTimeConverter();
        $result = $converter->toPhp('2024-01-15 14:30:45', PostgreSqlType::TIMESTAMP);
        self::assertInstanceOf(\DateTimeImmutable::class, $result);
    }
}
