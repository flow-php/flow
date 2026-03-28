<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use function Flow\ETL\DSL\{int_entry, row, rows, str_entry};
use Flow\ETL\Adapter\PostgreSql\{EntryTypesMap, LoaderOptions\InsertOptions, QueryBuilder\InsertQueryBuilder};
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class InsertQueryBuilderTest extends TestCase
{
    public function test_build_returns_typed_values() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
        ));

        self::assertCount(2, $params);
        self::assertInstanceOf(TypedValue::class, $params[0]);
        self::assertInstanceOf(TypedValue::class, $params[1]);
        self::assertSame(1, $params[0]->value);
        self::assertSame(ValueType::INT8, $params[0]->targetType);
        self::assertSame('Alice', $params[1]->value);
        self::assertSame(ValueType::TEXT, $params[1]->targetType);
    }

    public function test_build_simple_insert() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(rows(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            row(int_entry('id', 2), str_entry('name', 'Bob')),
        ));

        self::assertStringContainsString('INSERT INTO "users"', $query->toSql());
        self::assertStringContainsString('("id", "name")', $query->toSql());
        self::assertStringContainsString('($1, $2), ($3, $4)', $query->toSql());
        self::assertCount(4, $params);
    }

    public function test_build_with_null_values() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(rows(
            row(int_entry('id', 1), str_entry('name', null)),
        ));

        self::assertCount(2, $params);
        self::assertNull($params[1]);
    }

    public function test_build_with_skip_conflicts() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::skipConflicts()
        );

        self::assertStringContainsString('ON CONFLICT DO NOTHING', $query->toSql());
    }

    public function test_build_with_upsert_on_columns() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::upsertOnColumns(['id'])
        );

        $sql = $query->toSql();
        self::assertStringContainsString('ON CONFLICT ("id")', $sql);
        self::assertStringContainsString('DO UPDATE SET', $sql);
    }

    public function test_build_with_upsert_on_constraint() : void
    {
        $builder = new InsertQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            rows(row(int_entry('id', 1), str_entry('name', 'Alice'))),
            InsertOptions::upsertOnConstraint('users_pkey')
        );

        $sql = $query->toSql();
        self::assertStringContainsString('ON CONSTRAINT "users_pkey"', $sql);
        self::assertStringContainsString('DO UPDATE SET', $sql);
    }
}
