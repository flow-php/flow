<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\InsertQueryBuilder;
use Flow\PostgreSql\Client\Types\ValueConverters;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class InsertQueryBuilderTest extends TestCase
{
    public function test_build_converts_each_value_with_its_columns_converter(): void
    {
        [$_query, $params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => 'Alice', 'active' => true], ['id' => 2, 'name' => 'Bob', 'active' => false]],
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            ValueConverters::create(),
        );

        static::assertSame(['1', 'Alice', 't', '2', 'Bob', 'f'], $params->values);
    }

    public function test_build_simple_insert(): void
    {
        [$query, $params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
            schema(int_schema('id'), str_schema('name')),
            ValueConverters::create(),
        );

        static::assertStringContainsString('INSERT INTO "users"', $query->toSql());
        static::assertStringContainsString('("id", "name")', $query->toSql());
        static::assertStringContainsString('($1, $2), ($3, $4)', $query->toSql());
        static::assertCount(4, $params->values);
    }

    public function test_build_with_null_values(): void
    {
        [$_query, $params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => null]],
            schema(int_schema('id'), str_schema('name', nullable: true)),
            ValueConverters::create(),
        );

        static::assertSame(['1', null], $params->values);
    }

    public function test_build_with_skip_conflicts(): void
    {
        [$query, $_params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => 'Alice']],
            schema(int_schema('id'), str_schema('name')),
            ValueConverters::create(),
            InsertOptions::skipConflicts(),
        );

        static::assertStringContainsString('ON CONFLICT DO NOTHING', $query->toSql());
    }

    public function test_build_with_upsert_on_columns(): void
    {
        [$query, $_params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => 'Alice']],
            schema(int_schema('id'), str_schema('name')),
            ValueConverters::create(),
            InsertOptions::upsertOnColumns(['id']),
        );

        $sql = $query->toSql();
        static::assertStringContainsString('ON CONFLICT ("id")', $sql);
        static::assertStringContainsString('DO UPDATE SET', $sql);
    }

    public function test_build_with_upsert_on_constraint(): void
    {
        [$query, $_params] = (new InsertQueryBuilder('users', new EntryTypesMap()))->build(
            [['id' => 1, 'name' => 'Alice']],
            schema(int_schema('id'), str_schema('name')),
            ValueConverters::create(),
            InsertOptions::upsertOnConstraint('users_pkey'),
        );

        $sql = $query->toSql();
        static::assertStringContainsString('ON CONSTRAINT "users_pkey"', $sql);
        static::assertStringContainsString('DO UPDATE SET', $sql);
    }
}
