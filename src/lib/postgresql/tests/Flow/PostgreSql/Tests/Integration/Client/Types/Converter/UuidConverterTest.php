<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use function Flow\PostgreSql\DSL\{cast, column_type_uuid, func, literal, param, select};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class UuidConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_uuid_strings() : \Generator
    {
        yield 'lowercase' => ['550e8400-e29b-41d4-a716-446655440000'];
        yield 'uppercase' => ['550E8400-E29B-41D4-A716-446655440000'];
        yield 'nil uuid' => ['00000000-0000-0000-0000-000000000000'];
        yield 'max uuid' => ['ffffffff-ffff-ffff-ffff-ffffffffffff'];
    }

    public function test_gen_random_uuid() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(func('gen_random_uuid')->as('val'))->toSql());

        self::assertIsString($result);
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i',
            $result
        );
    }

    public function test_null_uuid() : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(literal(null), column_type_uuid())->as('val'))->toSql());

        self::assertNull($result);
    }

    #[DataProvider('provide_uuid_strings')]
    public function test_uuid_string_round_trip(string $input) : void
    {
        $result = $this->pgsqlContext()->client()->fetchScalar(select(cast(param(1), column_type_uuid())->as('val'))->toSql(), [$input]);

        self::assertIsString($result);
        self::assertSame(\strtolower($input), $result);
    }
}
