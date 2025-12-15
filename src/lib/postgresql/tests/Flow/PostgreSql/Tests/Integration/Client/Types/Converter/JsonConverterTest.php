<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\Types\Value\Json;
use PHPUnit\Framework\Attributes\DataProvider;

final class JsonConverterTest extends ConverterTestCase
{
    /**
     * @return \Generator<string, array{Json, string}>
     */
    public static function provide_json_objects() : \Generator
    {
        yield 'json object' => [
            Json::fromArray(['name' => 'John', 'age' => 30]),
            '{"name":"John","age":30}',
        ];
        yield 'empty array json' => [
            Json::fromArray([]),
            '[]',
        ];
        yield 'empty object json' => [
            Json::fromArray([], asObject: true),
            '{}',
        ];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_json_strings() : \Generator
    {
        yield 'empty object' => ['{}'];
        yield 'empty array' => ['[]'];
        yield 'simple object' => ['{"name":"John","age":30}'];
        yield 'simple array' => ['[1,2,3]'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_jsonb_strings() : \Generator
    {
        yield 'empty object' => ['{}', '{}'];
        yield 'empty array' => ['[]', '[]'];
        yield 'simple object' => ['{"name":"John","age":30}', '{"age": 30, "name": "John"}'];
        yield 'simple array' => ['[1,2,3]', '[1, 2, 3]'];
    }

    /**
     * @return \Generator<string, array{Json}>
     */
    public static function provide_nested_json() : \Generator
    {
        yield 'nested object' => [
            Json::fromArray([
                'user' => [
                    'name' => 'John',
                    'addresses' => [
                        ['city' => 'New York', 'country' => 'USA'],
                        ['city' => 'London', 'country' => 'UK'],
                    ],
                ],
            ]),
        ];
        yield 'array of objects' => [
            Json::fromArray([
                ['id' => 1, 'name' => 'First'],
                ['id' => 2, 'name' => 'Second'],
            ]),
        ];
    }

    #[DataProvider('provide_json_objects')]
    public function test_json_object_round_trip(Json $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::json AS val', [$input]);

        self::assertInstanceOf(Json::class, $result);
        self::assertSame($expected, $result->toString());
    }

    #[DataProvider('provide_json_strings')]
    public function test_json_string_round_trip(string $input) : void
    {
        $result = $this->fetchValue('SELECT $1::json AS val', [$input]);

        self::assertInstanceOf(Json::class, $result);
        self::assertSame($input, $result->toString());
    }

    #[DataProvider('provide_jsonb_strings')]
    public function test_jsonb_string_round_trip(string $input, string $expected) : void
    {
        $result = $this->fetchValue('SELECT $1::jsonb AS val', [$input]);

        self::assertInstanceOf(Json::class, $result);
        self::assertSame($expected, $result->toString());
    }

    #[DataProvider('provide_nested_json')]
    public function test_nested_json_round_trip(Json $input) : void
    {
        $result = $this->fetchValue('SELECT $1::json AS val', [$input]);

        self::assertInstanceOf(Json::class, $result);
        self::assertSame($input->toArray(), $result->toArray());
    }

    public function test_null_json() : void
    {
        $result = $this->fetchValue('SELECT NULL::json AS val');

        self::assertNull($result);
    }

    public function test_null_jsonb() : void
    {
        $result = $this->fetchValue('SELECT NULL::jsonb AS val');

        self::assertNull($result);
    }
}
