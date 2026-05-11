<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\Key;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\AST\Transformers\SortOrder;

final class PaginationTest extends FlowTestCase
{
    public function test_key_asc_creates_key_with_asc_order(): void
    {
        $key = Key::asc('id');

        static::assertSame('id', $key->column);
        static::assertSame(Order::ASC, $key->order);
    }

    public function test_key_converts_to_keyset_column(): void
    {
        $key = new Key('id', Order::ASC);
        $keysetColumn = $key->toKeysetColumn();

        static::assertSame('id', $keysetColumn->column);
        static::assertSame(SortOrder::ASC, $keysetColumn->order);
    }

    public function test_key_desc_creates_key_with_desc_order(): void
    {
        $key = Key::desc('id');

        static::assertSame('id', $key->column);
        static::assertSame(Order::DESC, $key->order);
    }

    public function test_keyset_converts_to_keyset_columns(): void
    {
        $keySet = new KeySet(new Key('id', Order::ASC), new Key('created_at', Order::DESC));

        $columns = $keySet->toKeysetColumns();

        static::assertCount(2, $columns);
        static::assertSame('id', $columns[0]->column);
        static::assertSame(SortOrder::ASC, $columns[0]->order);
        static::assertSame('created_at', $columns[1]->column);
        static::assertSame(SortOrder::DESC, $columns[1]->order);
    }

    public function test_keyset_requires_at_least_one_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('KeySet requires at least one key');

        new KeySet();
    }

    public function test_order_converts_to_sort_order(): void
    {
        static::assertSame(SortOrder::ASC, Order::ASC->toSortOrder());
        static::assertSame(SortOrder::DESC, Order::DESC->toSortOrder());
    }
}
