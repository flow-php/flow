<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\IntervalConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class IntervalConverterTest extends TestCase
{
    public function test_date_interval_with_all_components() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('P1Y2M3DT4H5M6S');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('1 year 2 months 3 days 04:05:06', $dbValue);
    }

    public function test_date_interval_with_days() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('P5D');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('5 days', $dbValue);
    }

    public function test_date_interval_with_months() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('P1M');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('1 month', $dbValue);
    }

    public function test_date_interval_with_time() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('PT2H30M15S');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('02:30:15', $dbValue);
    }

    public function test_date_interval_with_years() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('P2Y');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('2 years', $dbValue);
    }

    public function test_empty_date_interval() : void
    {
        $converter = new IntervalConverter();
        $interval = new \DateInterval('P0D');

        $dbValue = $converter->toDatabase($interval);
        self::assertSame('0', $dbValue);
    }

    public function test_non_interval_throws_exception() : void
    {
        $converter = new IntervalConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_null_handling() : void
    {
        $converter = new IntervalConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_passthrough() : void
    {
        $converter = new IntervalConverter();
        self::assertSame('1 day', $converter->toDatabase('1 day'));
    }

    public function test_supported_types() : void
    {
        $converter = new IntervalConverter();
        self::assertContains(PostgreSqlType::INTERVAL, $converter->supportedTypes());
    }
}
