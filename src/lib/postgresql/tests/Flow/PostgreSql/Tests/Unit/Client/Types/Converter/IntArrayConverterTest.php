<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\IntArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntArrayConverterTest extends TestCase
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
        yield 'integer array' => [[1, 2, 3], '{1,2,3}'];
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [[1, null, 3], '{1,NULL,3}'];
        yield 'single element' => [[42], '{42}'];
        yield 'negative integers' => [[-1, -2, -3], '{-1,-2,-3}'];
        yield 'zero' => [[0], '{0}'];
        yield 'zeros' => [[0, 0, 0], '{0,0,0}'];
        yield 'int2 boundaries' => [[-32768, 32767], '{-32768,32767}'];
        yield 'int4 boundaries' => [[-2147483648, 2147483647], '{-2147483648,2147483647}'];
        yield 'large array' => [array_fill(0, 10, 42), '{42,42,42,42,42,42,42,42,42,42}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'mixed positive negative' => [[-5, 0, 5], '{-5,0,5}'];
    }

    public function test_invalid_element_throws_exception() : void
    {
        $converter = new IntArrayConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([1, 'not an integer', 3]);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected) : void
    {
        $converter = new IntArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling() : void
    {
        $converter = new IntArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new IntArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::INT2_ARRAY, $types);
        self::assertContains(PostgreSqlType::INT4_ARRAY, $types);
        self::assertContains(PostgreSqlType::INT8_ARRAY, $types);
        self::assertCount(3, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected) : void
    {
        $converter = new IntArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
