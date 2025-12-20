<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
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

    public function test_datetime_to_database() : void
    {
        $converter = new DateTimeConverter();
        $date = new \DateTimeImmutable('2024-01-15 14:30:45.123456+00:00');

        $dbValue = $converter->toDatabase($date);
        self::assertNotNull($dbValue);
        self::assertSame('2024-01-15 14:30:45.123456+00:00', $dbValue);
    }

    public function test_non_datetime_throws_exception() : void
    {
        $converter = new DateTimeConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_null_handling() : void
    {
        $converter = new DateTimeConverter();
        self::assertNull($converter->toDatabase(null));
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
}
