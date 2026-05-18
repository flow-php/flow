<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\DateConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

final class DateConverterTest extends TestCase
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
        yield 'date only' => [new DateTimeImmutable('2024-01-15'), '2024-01-15'];
        yield 'date with time stripped' => [new DateTimeImmutable('2024-01-15 14:30:45'), '2024-01-15'];
        yield 'DateTime mutable' => [new DateTime('2024-01-15'), '2024-01-15'];
        yield 'leap year date' => [new DateTimeImmutable('2024-02-29'), '2024-02-29'];
        yield 'year start' => [new DateTimeImmutable('2024-01-01'), '2024-01-01'];
        yield 'year end' => [new DateTimeImmutable('2024-12-31'), '2024-12-31'];
        yield 'past date' => [new DateTimeImmutable('1900-01-01'), '1900-01-01'];
        yield 'future date' => [new DateTimeImmutable('2100-12-31'), '2100-12-31'];
        yield 'with timezone' => [
            new DateTimeImmutable('2024-01-15 00:00:00', new DateTimeZone('America/New_York')),
            '2024-01-15',
        ];
        yield 'string passthrough' => ['2024-01-15', '2024-01-15'];
        yield 'string different format' => ['2024/01/15', '2024/01/15'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new DateConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new DateConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new DateConverter();
        static::assertContains(ValueType::DATE, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(DateTimeInterface|string $input, string $expected): void
    {
        $converter = new DateConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
