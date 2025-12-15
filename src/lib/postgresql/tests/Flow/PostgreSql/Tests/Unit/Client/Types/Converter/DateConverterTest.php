<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\DateConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class DateConverterTest extends TestCase
{
    public function test_date_format_strips_time() : void
    {
        $converter = new DateConverter();
        $date = new \DateTimeImmutable('2024-01-15 14:30:45');

        $dbValue = $converter->toDatabase($date);
        self::assertSame('2024-01-15', $dbValue);
    }

    public function test_non_datetime_returns_empty() : void
    {
        $converter = new DateConverter();
        self::assertSame('', $converter->toDatabase(12345));
    }

    public function test_null_handling() : void
    {
        $converter = new DateConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new DateConverter();
        $date = new \DateTimeImmutable('2024-01-15');

        $dbValue = $converter->toDatabase($date);
        self::assertNotNull($dbValue);
        self::assertSame('2024-01-15', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::DATE);
        self::assertInstanceOf(\DateTimeInterface::class, $phpValue);
        self::assertSame('2024-01-15', $phpValue->format('Y-m-d'));
    }

    public function test_string_passthrough() : void
    {
        $converter = new DateConverter();
        self::assertSame('2024-01-15', $converter->toDatabase('2024-01-15'));
    }

    public function test_supported_types() : void
    {
        $converter = new DateConverter();
        self::assertContains(PostgreSqlType::DATE, $converter->supportedTypes());
    }

    public function test_to_php_returns_immutable() : void
    {
        $converter = new DateConverter();
        $result = $converter->toPhp('2024-01-15', PostgreSqlType::DATE);
        self::assertInstanceOf(\DateTimeImmutable::class, $result);
    }
}
