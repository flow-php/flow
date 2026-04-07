<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\FloatConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatConverterTest extends TestCase
{
    public static function provide_invalid_values() : \Generator
    {
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'array' => [['array']];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'positive float' => [3.14159, '3.14159'];
        yield 'negative float' => [-123.456, '-123.456'];
        yield 'zero' => [0.0, '0'];
        yield 'integer' => [42, '42'];
        yield 'negative integer' => [-42, '-42'];
        yield 'string numeric' => ['3.14', '3.14'];
        yield 'string negative' => ['-3.14', '-3.14'];
        yield 'NaN' => [\NAN, 'NaN'];
        yield 'Infinity' => [\INF, 'Infinity'];
        yield 'negative Infinity' => [-\INF, '-Infinity'];
        yield 'very small float' => [1.0E-10, '1.0E-10'];
        yield 'very large float' => [1.0E+100, '1.0E+100'];
        yield 'scientific notation string' => ['1.5e-10', '1.5e-10'];
        yield 'float4 max approx' => [3.4028235E+38, '3.4028235E+38'];
        yield 'float8 precision' => [1.7976931348623E+308, '1.7976931348623E+308'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value) : void
    {
        $converter = new FloatConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling() : void
    {
        $converter = new FloatConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new FloatConverter();
        $types = $converter->supportedTypes();

        self::assertContains(ValueType::FLOAT4, $types);
        self::assertContains(ValueType::FLOAT8, $types);
        self::assertCount(2, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(mixed $input, string $expected) : void
    {
        $converter = new FloatConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
