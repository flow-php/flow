<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\NoExecutionOrder;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final class NoExecutionOrderTest extends TestCase
{
    public function test_returns_tables_in_original_order(): void
    {
        $tableA = schema_table('alpha', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $tableB = schema_table('beta', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $tableC = schema_table('gamma', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $result = (new NoExecutionOrder())->order([$tableC, $tableA, $tableB]);

        static::assertSame([$tableC, $tableA, $tableB], $result);
    }
}
