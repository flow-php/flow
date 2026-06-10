<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\TimestampConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

final class TimestampConverterTest extends TestCase
{
    public static function provide_invalid_values(): Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new stdClass()];
    }

    public static function provide_valid_values(): Generator
    {
        yield 'utc stays utc' => [
            new DateTimeImmutable('2024-01-15 14:30:45.123456+00:00'),
            '2024-01-15 14:30:45.123456',
        ];
        yield 'positive offset normalized to utc' => [
            new DateTimeImmutable('2024-06-15 09:00:00.000000+02:00'),
            '2024-06-15 07:00:00.000000',
        ];
        yield 'negative offset normalized to utc' => [
            new DateTimeImmutable('2024-01-15 14:30:45.000000-05:00'),
            '2024-01-15 19:30:45.000000',
        ];
        yield 'offset rolls the day backwards' => [
            new DateTimeImmutable('2024-01-15 01:00:00.000000+02:00'),
            '2024-01-14 23:00:00.000000',
        ];
        yield 'mutable datetime normalized to utc' => [
            new DateTime('2024-06-15 09:00:00+02:00'),
            '2024-06-15 07:00:00.000000',
        ];
        yield 'max microseconds' => [
            new DateTimeImmutable('2024-01-15 12:00:00.999999+00:00'),
            '2024-01-15 12:00:00.999999',
        ];
        yield 'string passthrough' => ['2024-01-15 14:30:45', '2024-01-15 14:30:45'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new TimestampConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new TimestampConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new TimestampConverter();
        static::assertSame([ValueType::TIMESTAMP], $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(DateTimeInterface|string $input, string $expected): void
    {
        $converter = new TimestampConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
