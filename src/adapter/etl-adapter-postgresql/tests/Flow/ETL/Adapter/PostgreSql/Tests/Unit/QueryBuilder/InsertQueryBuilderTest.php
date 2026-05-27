<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\InsertOptions;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\InsertQueryBuilder;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class InsertQueryBuilderTest extends TestCase
{
    public function test_build_returns_typed_values(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$_query, $params] = $builder->build(rows(row(int_entry('id', 1), str_entry('name', 'Alice'))));

        static::assertCount(2, $params);

        $first = $params[0];
        $second = $params[1];
        static::assertInstanceOf(TypedValue::class, $first);
        static::assertInstanceOf(TypedValue::class, $second);
        static::assertSame(1, $first->value);
        static::assertSame(ValueType::INT8, $first->targetType);
        static::assertSame('Alice', $second->value);
        static::assertSame(ValueType::TEXT, $second->targetType);
    }

    public function test_build_simple_insert(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            row(int_entry('id', 2), str_entry('name', 'Bob')),
        ));

        static::assertStringContainsString('INSERT INTO "users"', $query->toSql());
        static::assertStringContainsString('("id", "name")', $query->toSql());
        static::assertStringContainsString('($1, $2), ($3, $4)', $query->toSql());
        static::assertCount(4, $params);
    }

    public function test_build_with_null_values(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$_query, $params] = $builder->build(rows(row(int_entry('id', 1), str_entry('name', null))));

        static::assertCount(2, $params);
        static::assertNull($params[1]);
    }

    public function test_build_with_skip_conflicts(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $_params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::skipConflicts(),
        );

        static::assertStringContainsString('ON CONFLICT DO NOTHING', $query->toSql());
    }

    public function test_build_with_upsert_on_columns(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $_params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::upsertOnColumns(['id']),
        );

        $sql = $query->toSql();
        static::assertStringContainsString('ON CONFLICT ("id")', $sql);
        static::assertStringContainsString('DO UPDATE SET', $sql);
    }

    public function test_build_with_upsert_on_constraint(): void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $_params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::upsertOnConstraint('users_pkey'),
        );

        $sql = $query->toSql();
        static::assertStringContainsString('ON CONSTRAINT "users_pkey"', $sql);
        static::assertStringContainsString('DO UPDATE SET', $sql);
    }
}
