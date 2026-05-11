<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\TimeConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TimeConverterTest extends TestCase
{
    public static function provide_invalid_values(): \Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'datetime with microseconds' => [new \DateTimeImmutable('14:30:45.123456'), '14:30:45.123456'];
        yield 'datetime morning' => [new \DateTimeImmutable('2024-01-15 09:00:00.000000'), '09:00:00.000000'];
        yield 'midnight' => [new \DateTimeImmutable('00:00:00.000000'), '00:00:00.000000'];
        yield 'end of day' => [new \DateTimeImmutable('23:59:59.999999'), '23:59:59.999999'];
        yield 'noon' => [new \DateTimeImmutable('12:00:00.000000'), '12:00:00.000000'];
        yield 'DateTime mutable' => [new \DateTime('14:30:45'), '14:30:45.000000'];
        yield 'date interval' => [new \DateInterval('PT10H30M15S'), '10:30:15'];
        yield 'date interval with zeros' => [new \DateInterval('PT0H5M0S'), '00:05:00'];
        yield 'date interval hours only' => [new \DateInterval('PT5H'), '05:00:00'];
        yield 'date interval seconds only' => [new \DateInterval('PT45S'), '00:00:45'];
        yield 'date interval over 24h' => [new \DateInterval('PT25H30M'), '25:30:00'];
        yield 'string passthrough' => ['14:30:45', '14:30:45'];
        yield 'string with timezone' => ['14:30:45+02:00', '14:30:45+02:00'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new TimeConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new TimeConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new TimeConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::TIME, $types);
        static::assertContains(ValueType::TIMETZ, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(\DateTimeInterface|\DateInterval|string $input, string $expected): void
    {
        $converter = new TimeConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
