<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\{Key, KeySet, Order};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\AST\Transformers\SortOrder;

final class PaginationTest extends FlowTestCase
{
    public function test_key_asc_creates_key_with_asc_order() : void
    {
        $key = Key::asc('id');

        self::assertSame('id', $key->column);
        self::assertSame(Order::ASC, $key->order);
    }

    public function test_key_converts_to_keyset_column() : void
    {
        $key = new Key('id', Order::ASC);
        $keysetColumn = $key->toKeysetColumn();

        self::assertSame('id', $keysetColumn->column);
        self::assertSame(SortOrder::ASC, $keysetColumn->order);
    }

    public function test_key_desc_creates_key_with_desc_order() : void
    {
        $key = Key::desc('id');

        self::assertSame('id', $key->column);
        self::assertSame(Order::DESC, $key->order);
    }

    public function test_keyset_converts_to_keyset_columns() : void
    {
        $keySet = new KeySet(
            new Key('id', Order::ASC),
            new Key('created_at', Order::DESC)
        );

        $columns = $keySet->toKeysetColumns();

        self::assertCount(2, $columns);
        self::assertSame('id', $columns[0]->column);
        self::assertSame(SortOrder::ASC, $columns[0]->order);
        self::assertSame('created_at', $columns[1]->column);
        self::assertSame(SortOrder::DESC, $columns[1]->order);
    }

    public function test_keyset_requires_at_least_one_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('KeySet requires at least one key');

        new KeySet();
    }

    public function test_order_converts_to_sort_order() : void
    {
        self::assertSame(SortOrder::ASC, Order::ASC->toSortOrder());
        self::assertSame(SortOrder::DESC, Order::DESC->toSortOrder());
    }
}
