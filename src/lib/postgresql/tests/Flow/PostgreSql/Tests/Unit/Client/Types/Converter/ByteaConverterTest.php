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
        yield 'simple text' => ['simple text', 'simple text'];
        yield 'hello' => ['hello', 'hello'];
        yield 'empty string' => ['', ''];
        yield 'binary with null byte' => ["hello\x00world", "hello\x00world"];
        yield 'binary with special bytes' => ["\x01\x02\x03\xff\xfe", "\x01\x02\x03\xff\xfe"];
        yield 'unicode characters' => ['日本語テスト', '日本語テスト'];
        yield 'emoji' => ['Hello 🎉 World', 'Hello 🎉 World'];
        yield 'newlines and tabs' => ["line1\nline2\ttab", "line1\nline2\ttab"];
        yield 'backslash' => ['path\\to\\file', 'path\\to\\file'];
        yield 'single quote' => ["it's a test", "it's a test"];
        yield 'double quote' => ['say "hello"', 'say "hello"'];
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
