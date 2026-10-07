<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Doctrine\Pagination\KeyValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function str_repeat;

final class KeyValuesTest extends FlowTestCase
{
    public function test_describe_names_every_key(): void
    {
        static::assertSame(
            "(created_at = '2025-01-01', id = 7)",
            KeyValues::of(
                pagination_key_set(pagination_key_asc('id'), pagination_key_asc('created_at')),
                ['key_created_at', 'key_id'],
                ['key_created_at' => '2025-01-01', 'key_id' => 7],
            )->describe(),
        );
    }

    public function test_describe_truncates_long_values(): void
    {
        static::assertSame(
            "(k = '" . str_repeat('a', 63) . '…)',
            KeyValues::of(
                pagination_key_set(pagination_key_asc('k')),
                ['key_k'],
                ['key_k' => str_repeat('a', 100)],
            )->describe(),
        );
    }

    public function test_equals_is_strict(): void
    {
        $keySet = pagination_key_set(pagination_key_asc('id'));

        static::assertTrue(KeyValues::of($keySet, ['key_id'], ['key_id' => 1])->equals(KeyValues::of(
            $keySet,
            ['key_id'],
            ['key_id' => 1],
        )));
        static::assertFalse(KeyValues::of($keySet, ['key_id'], ['key_id' => '1'])->equals(KeyValues::of(
            $keySet,
            ['key_id'],
            ['key_id' => 1],
        )));
    }

    public function test_missing_alias_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column "id" not found in result row for keyset pagination');

        KeyValues::of(pagination_key_set(pagination_key_asc('id')), ['key_id'], ['id' => 1]);
    }

    public function test_null_value_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'NULL value found in column "id" for keyset pagination; key columns must be non-null',
        );

        KeyValues::of(pagination_key_set(pagination_key_asc('id')), ['key_id'], ['key_id' => null]);
    }

    public function test_unsupported_value_type_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported value type "array" in column "id" for keyset pagination');

        KeyValues::of(pagination_key_set(pagination_key_asc('id')), ['key_id'], ['key_id' => [1]]);
    }
}
