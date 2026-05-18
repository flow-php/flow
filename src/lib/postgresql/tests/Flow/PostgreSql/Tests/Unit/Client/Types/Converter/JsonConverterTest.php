<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\JsonConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Stringable;

final class JsonConverterTest extends TestCase
{
    public static function provide_invalid_values(): Generator
    {
        yield 'integer' => [12345];
        yield 'float' => [3.14];
        yield 'boolean true' => [true];
        yield 'boolean false' => [false];
        yield 'object' => [new stdClass()];
    }

    public static function provide_valid_values(): Generator
    {
        yield 'JSON string simple' => ['{"name":"test","value":42}', '{"name":"test","value":42}'];
        yield 'JSON string key-value' => ['{"key":"value"}', '{"key":"value"}'];
        yield 'array with nested' => [
            ['name' => 'test', 'value' => 42, 'nested' => ['a' => 1]],
            '{"name":"test","value":42,"nested":{"a":1}}',
        ];
        yield 'empty object string' => ['{}', '{}'];
        yield 'empty array string' => ['[]', '[]'];
        yield 'empty array' => [[], '[]'];
        yield 'JSON array string' => ['[1,2,3]', '[1,2,3]'];
        yield 'indexed array' => [[1, 2, 3], '[1,2,3]'];
        yield 'null value in JSON string' => ['{"key":null}', '{"key":null}'];
        yield 'array with null' => [['key' => null], '{"key":null}'];
        yield 'boolean values in array' => [['active' => true, 'deleted' => false], '{"active":true,"deleted":false}'];
        yield 'numeric values' => [['int' => 42, 'float' => 3.14], '{"int":42,"float":3.14}'];
        yield 'unicode in JSON string' => ['{"text":"日本語"}', '{"text":"日本語"}'];
        yield 'array with unicode' => [['text' => '日本語'], '{"text":"\u65e5\u672c\u8a9e"}'];
        yield 'emoji in JSON string' => ['{"emoji":"👋🌍"}', '{"emoji":"👋🌍"}'];
        yield 'array with emoji' => [['emoji' => '👋🌍'], '{"emoji":"\ud83d\udc4b\ud83c\udf0d"}'];
        yield 'deeply nested' => [
            ['l1' => ['l2' => ['l3' => ['l4' => 'value']]]],
            '{"l1":{"l2":{"l3":{"l4":"value"}}}}',
        ];
        yield 'mixed types array' => [
            ['string' => 'text', 'number' => 42, 'bool' => true, 'null' => null, 'arr' => [1, 2]],
            '{"string":"text","number":42,"bool":true,"null":null,"arr":[1,2]}',
        ];
        yield 'special chars in string' => [['text' => 'quote"slash\\tab	newline
'], '{"text":"quote\"slash\\\\tab\\tnewline\\n"}'];
    }

    #[DataProvider('provide_invalid_values')]
    public function test_invalid_value_throws_exception(mixed $value): void
    {
        $converter = new JsonConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase($value);
    }

    public function test_null_handling(): void
    {
        $converter = new JsonConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_stringable_object(): void
    {
        $converter = new JsonConverter();
        $jsonObject = new class implements Stringable {
            public function __toString(): string
            {
                return '{"name":"test","value":42}';
            }
        };

        $dbValue = $converter->toDatabase($jsonObject);
        static::assertSame('{"name":"test","value":42}', $dbValue);
    }

    public function test_supported_types(): void
    {
        $converter = new JsonConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::JSON, $types);
        static::assertContains(ValueType::JSONB, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array|string $input, string $expected): void
    {
        $converter = new JsonConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
