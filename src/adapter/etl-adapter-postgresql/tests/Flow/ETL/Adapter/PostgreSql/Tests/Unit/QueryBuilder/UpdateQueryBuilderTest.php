<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\UpdateQueryBuilder;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class UpdateQueryBuilderTest extends TestCase
{
    public function test_build_returns_null_when_no_columns_to_update(): void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(row(int_entry('id', 1)), new UpdateOptions(['id']));

        static::assertNull($query);
        static::assertEmpty($params);
    }

    public function test_build_returns_typed_values(): void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$_query, $params] = $builder->build(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            new UpdateOptions(['id']),
        );

        static::assertCount(2, $params);
        static::assertInstanceOf(TypedValue::class, $params[0]);
        static::assertSame('Alice', $params[0]->value);
        static::assertSame(ValueType::TEXT, $params[0]->targetType);
        static::assertInstanceOf(TypedValue::class, $params[1]);
        static::assertSame(1, $params[1]->value);
        static::assertSame(ValueType::INT8, $params[1]->targetType);
    }

    public function test_build_simple_update(): void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1), str_entry('name', 'Alice')),
            new UpdateOptions(['id']),
        );

        static::assertNotNull($query);
        $sql = $query->toSql();
        static::assertStringContainsString('UPDATE users', $sql);
        static::assertStringContainsString('SET name = $1', $sql);
        static::assertStringContainsString('WHERE id = $2', $sql);
        static::assertCount(2, $params);
    }

    public function test_build_throws_when_no_primary_keys(): void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for UPDATE operation');

        $builder->build(row(int_entry('id', 1), str_entry('name', 'Alice')), new UpdateOptions([]));
    }

    public function test_build_throws_when_primary_key_not_in_row(): void
    {
        $builder = new UpdateQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary key "id" not found in row');

        $builder->build(row(str_entry('name', 'Alice')), new UpdateOptions(['id']));
    }

    public function test_build_with_multiple_primary_keys(): void
    {
        $builder = new UpdateQueryBuilder('order_items', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('order_id', 1), int_entry('product_id', 2), int_entry('quantity', 5)),
            new UpdateOptions(['order_id', 'product_id']),
        );

        static::assertNotNull($query);
        $sql = $query->toSql();
        static::assertStringContainsString('SET quantity = $1', $sql);
        static::assertStringContainsString('order_id = $2', $sql);
        static::assertStringContainsString('product_id = $3', $sql);
        static::assertCount(3, $params);
    }
}
