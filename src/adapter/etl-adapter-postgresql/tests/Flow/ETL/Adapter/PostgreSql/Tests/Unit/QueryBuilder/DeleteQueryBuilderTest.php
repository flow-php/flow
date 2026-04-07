<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use function Flow\ETL\DSL\{int_entry, row, str_entry};
use Flow\ETL\Adapter\PostgreSql\{EntryTypesMap, LoaderOptions\DeleteOptions, QueryBuilder\DeleteQueryBuilder};
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class DeleteQueryBuilderTest extends TestCase
{
    public function test_build_returns_typed_values() : void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1)),
            new DeleteOptions(['id'])
        );

        self::assertCount(1, $params);
        self::assertInstanceOf(TypedValue::class, $params[0]);
        self::assertSame(1, $params[0]->value);
        self::assertSame(ValueType::INT8, $params[0]->targetType);
    }

    public function test_build_simple_delete() : void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('id', 1)),
            new DeleteOptions(['id'])
        );

        $sql = $query->toSql();
        self::assertStringContainsString('DELETE FROM users', $sql);
        self::assertStringContainsString('WHERE id = $1', $sql);
        self::assertCount(1, $params);
    }

    public function test_build_throws_when_no_primary_keys() : void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for DELETE operation');

        $builder->build(
            row(int_entry('id', 1)),
            new DeleteOptions([])
        );
    }

    public function test_build_throws_when_primary_key_not_in_row() : void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary key "id" not found in row');

        $builder->build(
            row(str_entry('name', 'Alice')),
            new DeleteOptions(['id'])
        );
    }

    public function test_build_with_multiple_primary_keys() : void
    {
        $builder = new DeleteQueryBuilder('order_items', new EntryTypesMap());

        [$query, $params] = $builder->build(
            row(int_entry('order_id', 1), int_entry('product_id', 2)),
            new DeleteOptions(['order_id', 'product_id'])
        );

        $sql = $query->toSql();
        self::assertStringContainsString('DELETE FROM order_items', $sql);
        self::assertStringContainsString('order_id = $1', $sql);
        self::assertStringContainsString('product_id = $2', $sql);
        self::assertCount(2, $params);
    }
}
