<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\ByteaConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ByteaConverterTest extends TestCase
{
    public static function provide_invalid_values() : \Generator
    {
        yield 'integer' => [12345];
        yield 'array' => [['array']];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'simple text' => ['simple text', '\x' . \bin2hex('simple text')];
        yield 'hello' => ['hello', '\x' . \bin2hex('hello')];
        yield 'empty string' => ['', '\x'];
        yield 'binary with null byte' => ["hello\x00world", '\x' . \bin2hex("hello\x00world")];
        yield 'binary with special bytes' => ["\x01\x02\x03\xff\xfe", '\x' . \bin2hex("\x01\x02\x03\xff\xfe")];
        yield 'unicode characters' => ['日本語テスト', '\x' . \bin2hex('日本語テスト')];
        yield 'emoji' => ['Hello 🎉 World', '\x' . \bin2hex('Hello 🎉 World')];
        yield 'newlines and tabs' => ["line1\nline2\ttab", '\x' . \bin2hex("line1\nline2\ttab")];
        yield 'backslash' => ['path\\to\\file', '\x' . \bin2hex('path\\to\\file')];
        yield 'single quote' => ["it's a test", '\x' . \bin2hex("it's a test")];
        yield 'double quote' => ['say "hello"', '\x' . \bin2hex('say "hello"')];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value) : void
    {
        $converter = new ByteaConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling() : void
    {
        $converter = new ByteaConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new ByteaConverter();
        self::assertContains(ValueType::BYTEA, $converter->supportedTypes());
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(string $input, string $expected) : void
    {
        $converter = new ByteaConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
