<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\KeyValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function str_repeat;

final class KeyValuesTest extends FlowTestCase
{
    public function test_describe_names_every_key(): void
    {
        static::assertSame(
            "(u.created_at = '2025-01-01', id = 7)",
            KeyValues::of(
                pgsql_pagination_key_set(pgsql_pagination_key_asc('u.created_at'), pgsql_pagination_key_asc('id')),
                ['created_at' => '2025-01-01', 'id' => 7],
            )->describe(),
        );
    }

    public function test_describe_truncates_long_values(): void
    {
        static::assertSame(
            "(k = '" . str_repeat('a', 63) . '…)',
            KeyValues::of(pgsql_pagination_key_set(pgsql_pagination_key_asc('k')), ['k' => str_repeat(
                'a',
                100,
            )])->describe(),
        );
    }

    public function test_equals_is_strict(): void
    {
        $keySet = pgsql_pagination_key_set(pgsql_pagination_key_asc('id'));

        static::assertTrue(KeyValues::of($keySet, ['id' => 1])->equals(KeyValues::of($keySet, ['id' => 1])));
        static::assertFalse(KeyValues::of($keySet, ['id' => '1'])->equals(KeyValues::of($keySet, ['id' => 1])));
    }

    public function test_missing_column_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column "id" not found in result row for keyset pagination');

        KeyValues::of(pgsql_pagination_key_set(pgsql_pagination_key_asc('u.id')), ['other' => 1]);
    }

    public function test_null_value_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'NULL value found in column "id" for keyset pagination; key columns must be non-null',
        );

        KeyValues::of(pgsql_pagination_key_set(pgsql_pagination_key_asc('id')), ['id' => null]);
    }

    public function test_unsupported_value_type_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported value type "array" in column "id" for keyset pagination');

        KeyValues::of(pgsql_pagination_key_set(pgsql_pagination_key_asc('id')), ['id' => [1]]);
    }

    public function test_quoted_key_reads_the_unquoted_column(): void
    {
        static::assertSame([7], KeyValues::of(pgsql_pagination_key_set(pgsql_pagination_key_asc('"CreatedAt"')), [
            'CreatedAt' => 7,
        ])->values);
    }

    public function test_values_follow_the_key_order(): void
    {
        static::assertSame(
            ['2025-01-01', 7],
            KeyValues::of(
                pgsql_pagination_key_set(pgsql_pagination_key_asc('created_at'), pgsql_pagination_key_asc('id')),
                ['id' => 7, 'created_at' => '2025-01-01'],
            )->values,
        );
    }
}
