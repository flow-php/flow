<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\IntervalConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntervalConverterTest extends TestCase
{
    public static function provide_invalid_values() : \Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'years only' => [new \DateInterval('P2Y'), '2 years'];
        yield 'year singular' => [new \DateInterval('P1Y'), '1 year'];
        yield 'month singular' => [new \DateInterval('P1M'), '1 month'];
        yield 'months plural' => [new \DateInterval('P3M'), '3 months'];
        yield 'days only' => [new \DateInterval('P5D'), '5 days'];
        yield 'day singular' => [new \DateInterval('P1D'), '1 day'];
        yield 'time only' => [new \DateInterval('PT2H30M15S'), '02:30:15'];
        yield 'hours only' => [new \DateInterval('PT5H'), '05:00:00'];
        yield 'minutes only' => [new \DateInterval('PT30M'), '00:30:00'];
        yield 'seconds only' => [new \DateInterval('PT45S'), '00:00:45'];
        yield 'all components' => [new \DateInterval('P1Y2M3DT4H5M6S'), '1 year 2 months 3 days 04:05:06'];
        yield 'years and months' => [new \DateInterval('P2Y6M'), '2 years 6 months'];
        yield 'days and time' => [new \DateInterval('P10DT12H'), '10 days 12:00:00'];
        yield 'empty interval' => [new \DateInterval('P0D'), '0'];
        yield 'large values' => [new \DateInterval('P100Y'), '100 years'];
        yield 'string passthrough' => ['1 day', '1 day'];
        yield 'string interval format' => ['1 year 2 months 3 days', '1 year 2 months 3 days'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value) : void
    {
        $converter = new IntervalConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling() : void
    {
        $converter = new IntervalConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new IntervalConverter();
        self::assertContains(ValueType::INTERVAL, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(\DateInterval|string $input, string $expected) : void
    {
        $converter = new IntervalConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
