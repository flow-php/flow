<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\JsonArrayConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function fclose;
use function fopen;

final class JsonArrayConverterTest extends TestCase
{
    public static function provide_non_array_values(): Generator
    {
        yield 'string' => ['not an array', '{}'];
        yield 'integer' => [12345, '{}'];
        yield 'float' => [3.14, '{}'];
        yield 'boolean true' => [true, '{}'];
        yield 'boolean false' => [false, '{}'];
        yield 'object' => [new stdClass(), '{}'];
    }

    public static function provide_valid_values(): Generator
    {
        yield 'empty array' => [[], '{}'];
        yield 'array with null' => [
            [['key' => 'value'], null, ['key2' => 'value2']],
            '{"{\"key\":\"value\"}",NULL,"{\"key2\":\"value2\"}"}',
        ];
        yield 'single element' => [[['key' => 'value']], '{"{\"key\":\"value\"}"}'];
        yield 'all nulls' => [[null, null, null], '{NULL,NULL,NULL}'];
        yield 'empty objects' => [[[], []], '{"[]","[]"}'];
        yield 'nested arrays' => [[[1, 2, 3], [4, 5, 6]], '{"[1,2,3]","[4,5,6]"}'];
        yield 'boolean values' => [
            [['active' => true], ['active' => false]],
            '{"{\"active\":true}","{\"active\":false}"}',
        ];
        yield 'numeric values' => [[['count' => 42], ['price' => 9.99]], '{"{\"count\":42}","{\"price\":9.99}"}'];
        yield 'mixed types' => [
            [['str' => 'text', 'num' => 1, 'bool' => true]],
            '{"{\"str\":\"text\",\"num\":1,\"bool\":true}"}',
        ];
    }

    public function test_invalid_element_throws_exception(): void
    {
        $converter = new JsonArrayConverter();
        $resource = fopen('php://memory', 'rb');

        try {
            $this->expectException(ValueConversionException::class);
            $converter->toDatabase([['key' => 'value'], $resource]);
        } finally {
            fclose($resource);
        }
    }

    public function test_json_array_to_database(): void
    {
        $converter = new JsonArrayConverter();
        $array = [
            ['key' => 'value1'],
            ['key' => 'value2'],
        ];

        $dbValue = $converter->toDatabase($array);
        static::assertSame('{"{\"key\":\"value1\"}","{\"key\":\"value2\"}"}', $dbValue);
    }

    #[DataProvider('provide_non_array_values')]
    public function test_non_array_returns_empty_braces(mixed $input, string $expected): void
    {
        $converter = new JsonArrayConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }

    public function test_null_handling(): void
    {
        $converter = new JsonArrayConverter();
        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types(): void
    {
        $converter = new JsonArrayConverter();
        $types = $converter->supportedTypes();

        static::assertContains(ValueType::JSON_ARRAY, $types);
        static::assertContains(ValueType::JSONB_ARRAY, $types);
        static::assertCount(2, $types);
    }

    #[DataProvider('provide_valid_values')]
    public function test_to_database(array $input, string $expected): void
    {
        $converter = new JsonArrayConverter();
        static::assertSame($expected, $converter->toDatabase($input));
    }
}
