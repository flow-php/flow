<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StringConverterTest extends TestCase
{
    public static function provide_invalid_values() : \Generator
    {
        yield 'array' => [['array']];
        yield 'object' => [new \stdClass()];
    }

    public static function provide_valid_values() : \Generator
    {
        yield 'string' => ['hello world', 'hello world'];
        yield 'empty string' => ['', ''];
        yield 'integer' => [42, '42'];
        yield 'float' => [3.14, '3.14'];
        yield 'boolean true' => [true, '1'];
        yield 'boolean false' => [false, ''];
        yield 'unicode' => ['日本語テキスト', '日本語テキスト'];
        yield 'emoji' => ['Hello 👋 World 🌍', 'Hello 👋 World 🌍'];
        yield 'special chars' => ['!@#$%^&*()_+-=[]{}|;:,.<>?', '!@#$%^&*()_+-=[]{}|;:,.<>?'];
        yield 'single quote' => ["It's a test", "It's a test"];
        yield 'double quote' => ['"quoted"', '"quoted"'];
        yield 'backslash' => ['path\\to\\file', 'path\\to\\file'];
        yield 'newline' => ["line1\nline2", "line1\nline2"];
        yield 'tab' => ["col1\tcol2", "col1\tcol2"];
        yield 'carriage return' => ["line1\r\nline2", "line1\r\nline2"];
        yield 'whitespace only' => ['   ', '   '];
        yield 'null byte' => ["before\x00after", "before\x00after"];
        yield 'mixed unicode and ascii' => ['Hello мир 世界', 'Hello мир 世界'];
        yield 'negative integer' => [-42, '-42'];
        yield 'negative float' => [-3.14, '-3.14'];
        yield 'scientific notation float' => [1.5e10, '15000000000'];
        yield 'zero' => [0, '0'];
        yield 'zero float' => [0.0, '0'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value) : void
    {
        $converter = new StringConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling() : void
    {
        $converter = new StringConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_stringable_object_conversion() : void
    {
        $converter = new StringConverter();
        $stringable = new class {
            public function __toString() : string
            {
                return 'stringable';
            }
        };
        self::assertSame('stringable', $converter->toDatabase($stringable));
    }

    public function test_supported_types() : void
    {
        $converter = new StringConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::TEXT, $types);
        self::assertContains(PostgreSqlType::VARCHAR, $types);
        self::assertContains(PostgreSqlType::CHAR, $types);
        self::assertContains(PostgreSqlType::BPCHAR, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(mixed $input, string $expected) : void
    {
        $converter = new StringConverter();
        self::assertSame($expected, $converter->toDatabase($input));
    }
}
