<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_json, column_type_jsonb, literal, param, select, typed};
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class JsonConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{array<mixed>, string}>
     */
    public static function provide_json_arrays() : \Generator
    {
        yield 'json array' => [
            ['name' => 'John', 'age' => 30],
            '{"name":"John","age":30}',
        ];
        yield 'empty array json' => [
            [],
            '[]',
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
     * @return \Generator<string, array{array<mixed>}>
     */
    public static function provide_nested_json() : \Generator
    {
        yield 'nested object' => [
            [
                'user' => [
                    'name' => 'John',
                    'addresses' => [
                        ['city' => 'New York', 'country' => 'USA'],
                        ['city' => 'London', 'country' => 'UK'],
                    ],
                ],
            ],
        ];
        yield 'array of objects' => [
            [
                ['id' => 1, 'name' => 'First'],
                ['id' => 2, 'name' => 'Second'],
            ],
        ];
    }

    /**
     * @param array<mixed> $input
     */
    #[DataProvider('provide_json_arrays')]
    public function test_json_array_round_trip(array $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_json())->as('val'))->toSql(), [typed($input, ValueType::JSON)]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    #[DataProvider('provide_json_strings')]
    public function test_json_string_round_trip(string $input) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_json())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame($input, $result);
    }

    #[DataProvider('provide_jsonb_strings')]
    public function test_jsonb_string_round_trip(string $input, string $expected) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_jsonb())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame($expected, $result);
    }

    /**
     * @param array<mixed> $input
     */
    #[DataProvider('provide_nested_json')]
    public function test_nested_json_round_trip(array $input) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_json())->as('val'))->toSql(), [typed($input, ValueType::JSON)]);

        self::assertIsString($result);
        self::assertSame($input, \json_decode($result, true, 512, \JSON_THROW_ON_ERROR));
    }

    public function test_null_json() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_json())->as('val'))->toSql());

        self::assertNull($result);
    }

    public function test_null_jsonb() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_jsonb())->as('val'))->toSql());

        self::assertNull($result);
    }
}
