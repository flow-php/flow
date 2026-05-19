<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Types\Converter;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_uuid;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function strtolower;

final class UuidConverterTest extends PostgreSqlTestCase
{
    /**
     * @return \Generator<string, array{string}>
     */
    public static function provide_uuid_strings(): Generator
    {
        yield 'lowercase' => ['550e8400-e29b-41d4-a716-446655440000'];
        yield 'uppercase' => ['550E8400-E29B-41D4-A716-446655440000'];
        yield 'nil uuid' => ['00000000-0000-0000-0000-000000000000'];
        yield 'max uuid' => ['ffffffff-ffff-ffff-ffff-ffffffffffff'];
    }

    public function test_gen_random_uuid(): void
    {
        static::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i',
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalarString(select(func('gen_random_uuid')->as('val'))->toSql()),
        );
    }

    public function test_null_uuid(): void
    {
        static::assertNull(
            $this
                ->pgsqlContext()
                ->client()
                ->fetchScalar(select(cast(literal(null), column_type_uuid())->as('val'))->toSql()),
        );
    }

    #[DataProvider('provide_uuid_strings')]
    public function test_uuid_string_round_trip(string $input): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarString(select(cast(param(1), column_type_uuid())->as('val'))->toSql(), [$input]);

        static::assertSame(strtolower($input), $result);
    }
}
