<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use function Flow\ETL\DSL\{int_entry, row, str_entry};
use Flow\ETL\Adapter\PostgreSql\{EntryTypesMap, LoaderOptions\UpdateOptions, QueryBuilder\UpdateQueryBuilder};
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class UpdateQueryBuilderTest extends TestCase
{
    public function test_build_returns_null_when_no_columns_to_update() : void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1)),
            new UpdateOptions(['id'])
        );

        self::assertNull($query);
        self::assertEmpty($params);
    }

    public function test_build_returns_typed_values() : void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            new UpdateOptions(['id'])
        );

        self::assertCount(2, $params);
        self::assertInstanceOf(TypedValue::class, $params[0]);
        self::assertSame('Alice', $params[0]->value);
        self::assertSame(ValueType::TEXT, $params[0]->targetType);
        self::assertInstanceOf(TypedValue::class, $params[1]);
        self::assertSame(1, $params[1]->value);
        self::assertSame(ValueType::INT8, $params[1]->targetType);
    }

    public function test_build_simple_update() : void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            new UpdateOptions(['id'])
        );

        self::assertNotNull($query);
        $sql = $query->toSql();
        self::assertStringContainsString('UPDATE users', $sql);
        self::assertStringContainsString('SET name = $1', $sql);
        self::assertStringContainsString('WHERE id = $2', $sql);
        self::assertCount(2, $params);
    }

    public function test_build_throws_when_no_primary_keys() : void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for UPDATE operation');

        $builder->build(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            new UpdateOptions([])
        );
    }

    public function test_build_throws_when_primary_key_not_in_row() : void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary key "id" not found in row');

        $builder->build(
            row(str_entry('name', 'Alice')),
            new UpdateOptions(['id'])
        );
    }

    public function test_build_with_multiple_primary_keys() : void
    {
        $builder = new UpdateQueryBuilder('order_items', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('order_id', 1), int_entry('product_id', 2), int_entry('quantity', 5)),
            new UpdateOptions(['order_id', 'product_id'])
        );

        self::assertNotNull($query);
        $sql = $query->toSql();
        self::assertStringContainsString('SET quantity = $1', $sql);
        self::assertStringContainsString('order_id = $2', $sql);
        self::assertStringContainsString('product_id = $3', $sql);
        self::assertCount(3, $params);
    }
}
