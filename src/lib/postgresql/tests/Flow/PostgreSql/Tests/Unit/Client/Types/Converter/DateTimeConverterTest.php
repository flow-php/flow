<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\DateTimeConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DateTimeConverterTest extends TestCase
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
        yield 'datetime with microseconds UTC' => [
            new \DateTimeImmutable('2024-01-15 14:30:45.123456+00:00'),
            '2024-01-15 14:30:45.123456+00:00',
        ];
        yield 'datetime with timezone offset' => [
            new \DateTimeImmutable('2024-06-15 09:00:00.000000+02:00'),
            '2024-06-15 09:00:00.000000+02:00',
        ];
        yield 'DateTime mutable' => [new \DateTime('2024-01-15 14:30:45+00:00'), '2024-01-15 14:30:45.000000+00:00'];
        yield 'negative timezone offset' => [
            new \DateTimeImmutable('2024-01-15 14:30:45.000000-05:00'),
            '2024-01-15 14:30:45.000000-05:00',
        ];
        yield 'midnight' => [
            new \DateTimeImmutable('2024-01-15 00:00:00.000000+00:00'),
            '2024-01-15 00:00:00.000000+00:00',
        ];
        yield 'end of day' => [
            new \DateTimeImmutable('2024-01-15 23:59:59.999999+00:00'),
            '2024-01-15 23:59:59.999999+00:00',
        ];
        yield 'max microseconds' => [
            new \DateTimeImmutable('2024-01-15 12:00:00.999999+00:00'),
            '2024-01-15 12:00:00.999999+00:00',
        ];
        yield 'zero microseconds' => [
            new \DateTimeImmutable('2024-01-15 12:00:00.000000+00:00'),
            '2024-01-15 12:00:00.000000+00:00',
        ];
        yield 'string passthrough' => ['2024-01-15 14:30:45', '2024-01-15 14:30:45'];
        yield 'string with timezone' => ['2024-01-15 14:30:45+02:00', '2024-01-15 14:30:45+02:00'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new DateTimeConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new DateTimeConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new DateTimeConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::TIMESTAMP, $types);
        static::assertContains(ValueType::TIMESTAMPTZ, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(\DateTimeInterface|string $input, string $expected): void
    {
        $converter = new DateTimeConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
