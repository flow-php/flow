<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_json;
use function Flow\PostgreSql\DSL\column_type_jsonb;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\typed;
use function json_decode;

use const JSON_THROW_ON_ERROR;

final class JsonConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{array<mixed>, string}>
     */
    public static function provide_json_arrays(): Generator
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
    public static function provide_json_strings(): Generator
    {
        yield 'empty object' => ['{}'];
        yield 'empty array' => ['[]'];
        yield 'simple object' => ['{"name":"John","age":30}'];
        yield 'simple array' => ['[1,2,3]'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function provide_jsonb_strings(): Generator
    {
        yield 'empty object' => ['{}', '{}'];
        yield 'empty array' => ['[]', '[]'];
        yield 'simple object' => ['{"name":"John","age":30}', '{"age": 30, "name": "John"}'];
        yield 'simple array' => ['[1,2,3]', '[1, 2, 3]'];
    }

    /**
     * @return \Generator<string, array{array<mixed>}>
     */
    public static function provide_nested_json(): Generator
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
    public function test_json_array_round_trip(array $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_json())->as('val'))->toSql(), [typed(
                $input,
                ValueType::JSON,
            )]);

        static::assertSame($expected, $result);
    }

    #[DataProvider('provide_json_strings')]
    public function test_json_string_round_trip(string $input): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_json())->as('val'))->toSql(), [$input]);

        static::assertSame($input, $result);
    }

    #[DataProvider('provide_jsonb_strings')]
    public function test_jsonb_string_round_trip(string $input, string $expected): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_jsonb())->as('val'))->toSql(), [$input]);

        static::assertSame($expected, $result);
    }

    /**
     * @param array<mixed> $input
     */
    #[DataProvider('provide_nested_json')]
    public function test_nested_json_round_trip(array $input): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_json())->as('val'))->toSql(), [typed(
                $input,
                ValueType::JSON,
            )]);

        static::assertSame($input, json_decode($result, true, 512, JSON_THROW_ON_ERROR));
    }

    public function test_null_json(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_json())->as('val'))->toSql()),
        );
    }

    public function test_null_jsonb(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_jsonb())->as('val'))->toSql()),
        );
    }
}
