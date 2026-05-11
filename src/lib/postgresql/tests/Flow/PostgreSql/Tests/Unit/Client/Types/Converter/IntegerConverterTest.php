<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\IntegerConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntegerConverterTest extends TestCase
{
    public static function provide_invalid_values(): \Generator
    {
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'float' => [3.7];
        yield 'array' => [['array']];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'positive integer' => [42, '42'];
        yield 'negative integer' => [-123, '-123'];
        yield 'zero' => [0, '0'];
        yield 'int2 max' => [32767, '32767'];
        yield 'int2 min' => [-32768, '-32768'];
        yield 'int4 max' => [2147483647, '2147483647'];
        yield 'int4 min' => [-2147483648, '-2147483648'];
        yield 'large int8' => [9223372036854775807, '9223372036854775807'];
        yield 'negative int8' => [-9223372036854775807, '-9223372036854775807'];
        yield 'string numeric' => ['42', '42'];
        yield 'string negative' => ['-123', '-123'];
        yield 'string zero' => ['0', '0'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new IntegerConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new IntegerConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new IntegerConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::INT2, $types);
        static::assertContains(ValueType::INT4, $types);
        static::assertContains(ValueType::INT8, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(mixed $input, string $expected): void
    {
        $converter = new IntegerConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
