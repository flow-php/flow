<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\QueryBuilder;

use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\QueryBuilder\DeleteQueryBuilder;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DeleteQueryBuilderTest extends TestCase
{
    public function test_build_returns_typed_values(): void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        [$_query, $params] = $builder->build(['id' => 1], schema(int_schema('id')), new DeleteOptions(['id']));

        static::assertCount(1, $params);
        static::assertInstanceOf(TypedValue::class, $params[0]);
        static::assertSame(1, $params[0]->value);
        static::assertSame(ValueType::INT8, $params[0]->targetType);
    }

    public function test_build_simple_delete(): void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        [$query, $params] = $builder->build(['id' => 1], schema(int_schema('id')), new DeleteOptions(['id']));

        $sql = $query->toSql();
        static::assertStringContainsString('DELETE FROM users', $sql);
        static::assertStringContainsString('WHERE id = $1', $sql);
        static::assertCount(1, $params);
    }

    public function test_build_throws_when_no_primary_keys(): void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for DELETE operation');

        $builder->build(['id' => 1], schema(int_schema('id')), new DeleteOptions([]));
    }

    public function test_build_throws_when_primary_key_not_in_row(): void
    {
        $builder = new DeleteQueryBuilder('users', new EntryTypesMap());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary key "id" not found in row');

        $builder->build(['name' => 'Alice'], schema(str_schema('name')), new DeleteOptions(['id']));
    }

    public function test_build_with_multiple_primary_keys(): void
    {
        $builder = new DeleteQueryBuilder('order_items', new EntryTypesMap());

        [$query, $params] = $builder->build(
            ['order_id' => 1, 'product_id' => 2],
            schema(int_schema('order_id'), int_schema('product_id')),
            new DeleteOptions(['order_id', 'product_id']),
        );

        $sql = $query->toSql();
        static::assertStringContainsString('DELETE FROM order_items', $sql);
        static::assertStringContainsString('order_id = $1', $sql);
        static::assertStringContainsString('product_id = $2', $sql);
        static::assertCount(2, $params);
    }
}
