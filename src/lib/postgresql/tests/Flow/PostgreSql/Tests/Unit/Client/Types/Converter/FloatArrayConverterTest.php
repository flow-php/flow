<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\FloatArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatArrayConverterTest extends TestCase
{
    public static function provide_non_array_values() : \Generator
    {
        yield 'string' => ['not an array', '{}'];
        yield 'integer' => [12345, '{}'];
        yield 'float' => [3.14, '{}'];
        yield 'boolean true' => [true, '{}'];
        yield 'boolean false' => [false, '{}'];
        yield 'object' => [new \stdClass(), '{}'];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'float array' => [[1.5, 2.7, 3.9], '{1.5,2.7,3.9}'];
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [[1.5, null, 3.9], '{1.5,NULL,3.9}'];
        yield 'single element' => [[3.14], '{3.14}'];
        yield 'negative floats' => [[-1.5, -2.7, -3.9], '{-1.5,-2.7,-3.9}'];
        yield 'zero' => [[0.0], '{0}'];
        yield 'zeros' => [[0.0, 0.0, 0.0], '{0,0,0}'];
        yield 'integers as floats' => [[1.0, 2.0, 3.0], '{1,2,3}'];
        yield 'mixed int and float' => [[1, 2.5, 3], '{1,2.5,3}'];
        yield 'small floats' => [[0.001, 0.0001], '{0.001,0.0001}'];
        yield 'large floats' => [[1000000.5, 9999999.9], '{1000000.5,9999999.9}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'mixed positive negative' => [[-1.5, 0.0, 1.5], '{-1.5,0,1.5}'];
    }

    public function test_invalid_element_throws_exception() : void
    {
        $converter = new FloatArrayConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([1.5, 'not a float', 3.9]);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected) : void
    {
        $converter = new FloatArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling() : void
    {
        $converter = new FloatArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new FloatArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::FLOAT4_ARRAY, $types);
        self::assertContains(PostgreSqlType::FLOAT8_ARRAY, $types);
        self::assertCount(2, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected) : void
    {
        $converter = new FloatArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
