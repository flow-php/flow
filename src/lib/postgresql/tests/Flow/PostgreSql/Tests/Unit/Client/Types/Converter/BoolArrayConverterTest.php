<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\BoolArrayConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BoolArrayConverterTest extends TestCase
{
    public static function provide_non_array_values(): \Generator
    {
        yield 'string' => ['not an array', '{}'];
        yield 'integer' => [12345, '{}'];
        yield 'float' => [3.14, '{}'];
        yield 'boolean true' => [true, '{}'];
        yield 'boolean false' => [false, '{}'];
        yield 'object' => [new \stdClass(), '{}'];
    }

    public static function provide_valid_values(): \Generator
    {
        yield 'boolean array' => [[true, false, true], '{t,f,t}'];
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [[true, null, false], '{t,NULL,f}'];
        yield 'single true' => [[true], '{t}'];
        yield 'single false' => [[false], '{f}'];
        yield 'all true' => [[true, true, true], '{t,t,t}'];
        yield 'all false' => [[false, false, false], '{f,f,f}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'alternating' => [[true, false, true, false], '{t,f,t,f}'];
        yield 'large array' => [array_fill(0, 10, true), '{t,t,t,t,t,t,t,t,t,t}'];
    }

    public function test_invalid_element_throws_exception(): void
    {
        $converter = new BoolArrayConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([true, 'not a boolean', false]);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected): void
    {
        $converter = new BoolArrayConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling(): void
    {
        $converter = new BoolArrayConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new BoolArrayConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::BOOL_ARRAY, $types);
        static::assertCount(1, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected): void
    {
        $converter = new BoolArrayConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
