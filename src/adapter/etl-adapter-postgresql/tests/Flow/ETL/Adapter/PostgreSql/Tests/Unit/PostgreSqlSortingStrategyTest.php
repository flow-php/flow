<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlSortingStrategy;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function Flow\ETL\Adapter\PostgreSql\pgsql_sort_strategy;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PostgreSqlSortingStrategyTest extends TestCase
{
    public function test_explicit_type_metadata_overrides_mapping(): void
    {
        $schema = schema(int_schema('a', metadata: PostgreSqlMetadata::type('zzz_custom')), int_schema('b'));

        static::assertSame(['b', 'a'], array_keys($schema->sort(new PostgreSqlSortingStrategy())->definitions()));
    }

    public function test_groups_by_type_then_name(): void
    {
        $schema = schema(str_schema('label'), int_schema('count'), bool_schema('active'), int_schema('amount'));

        static::assertSame(
            ['active', 'amount', 'count', 'label'],
            array_keys($schema->sort(new PostgreSqlSortingStrategy())->definitions()),
        );
    }

    public function test_primary_key_columns_come_first(): void
    {
        $schema = schema(
            str_schema('name'),
            int_schema('id', metadata: PostgreSqlMetadata::primaryKey()),
            str_schema('email'),
        );

        static::assertSame(['id', 'email', 'name'], array_keys($schema->sort(pgsql_sort_strategy())->definitions()));
    }
}
