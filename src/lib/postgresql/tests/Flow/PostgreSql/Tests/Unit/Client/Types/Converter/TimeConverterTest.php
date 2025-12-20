<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\TimeConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class TimeConverterTest extends TestCase
{
    public function test_non_datetime_throws_exception() : void
    {
        $converter = new TimeConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_null_handling() : void
    {
        $converter = new TimeConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_passthrough() : void
    {
        $converter = new TimeConverter();
        self::assertSame('14:30:45', $converter->toDatabase('14:30:45'));
    }

    public function test_supported_types() : void
    {
        $converter = new TimeConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::TIME, $types);
        self::assertContains(PostgreSqlType::TIMETZ, $types);
    }

    public function test_time_format() : void
    {
        $converter = new TimeConverter();
        $time = new \DateTimeImmutable('2024-01-15 09:00:00.000000');

        $dbValue = $converter->toDatabase($time);
        self::assertSame('09:00:00.000000', $dbValue);
    }

    public function test_time_to_database() : void
    {
        $converter = new TimeConverter();
        $time = new \DateTimeImmutable('14:30:45.123456');

        $dbValue = $converter->toDatabase($time);
        self::assertNotNull($dbValue);
        self::assertSame('14:30:45.123456', $dbValue);
    }
}
