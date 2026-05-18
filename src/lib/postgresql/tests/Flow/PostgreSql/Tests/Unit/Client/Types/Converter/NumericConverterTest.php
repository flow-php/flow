<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\NumericConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

final class NumericConverterTest extends TestCase
{
    public static function provide_invalid_values(): Generator
    {
        yield 'array' => [['array']];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new stdClass()];
    }

    public static function provide_valid_values(): Generator
    {
        yield 'integer' => [42, '42'];
        yield 'negative integer' => [-42, '-42'];
        yield 'zero integer' => [0, '0'];
        yield 'float' => [3.14159, '3.14159'];
        yield 'negative float' => [-3.14159, '-3.14159'];
        yield 'zero float' => [0.0, '0'];
        yield 'string numeric' => ['999.99', '999.99'];
        yield 'string negative' => ['-999.99', '-999.99'];
        yield 'large precision string' => [
            '12345678901234567890.12345678901234567890',
            '12345678901234567890.12345678901234567890',
        ];
        yield 'scientific notation string' => ['1.23e10', '1.23e10'];
        yield 'very high precision' => ['0.123456789012345678901234567890', '0.123456789012345678901234567890'];
        yield 'integer as string' => ['42', '42'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new NumericConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new NumericConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new NumericConverter();
        static::assertContains(ValueType::NUMERIC, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(mixed $input, string $expected): void
    {
        $converter = new NumericConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
