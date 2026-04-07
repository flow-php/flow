<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\TextArrayConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TextArrayConverterTest extends TestCase
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
        yield 'text array' => [['a', 'b', 'c'], '{a,b,c}'];
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [['a', null, 'c'], '{a,NULL,c}'];
        yield 'single element' => [['hello'], '{hello}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'empty strings' => [['', '', ''], '{"","",""}'];
        yield 'single empty string' => [[''], '{""}'];
        yield 'words with spaces' => [['hello world', 'foo bar'], '{"hello world","foo bar"}'];
        yield 'unicode' => [['日本語', '中文'], '{日本語,中文}'];
        yield 'numeric strings' => [['1', '2', '3'], '{1,2,3}'];
        yield 'mixed content' => [['text', '123', 'more text'], '{text,123,"more text"}'];
        yield 'special chars comma' => [['a,b', 'c,d'], '{"a,b","c,d"}'];
        yield 'special chars braces' => [['{a}', '{b}'], '{"{a}","{b}"}'];
        yield 'double quotes' => [['"quoted"'], '{"\"quoted\""}'];
        yield 'backslash' => [['path\\to'], '{"path\\\\to"}'];
        yield 'null literal string' => [['NULL', 'null'], '{"NULL","null"}'];
    }

    public function test_invalid_element_throws_exception() : void
    {
        $converter = new TextArrayConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['a', ['nested' => 'array'], 'c']);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected) : void
    {
        $converter = new TextArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling() : void
    {
        $converter = new TextArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new TextArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(ValueType::TEXT_ARRAY, $types);
        self::assertContains(ValueType::VARCHAR_ARRAY, $types);
        self::assertCount(2, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected) : void
    {
        $converter = new TextArrayConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
