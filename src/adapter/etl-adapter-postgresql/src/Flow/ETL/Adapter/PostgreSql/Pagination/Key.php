<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Pagination;

use Flow\PostgreSql\AST\Transformers\KeysetColumn;

use function Flow\PostgreSql\DSL\sql_keyset_column;

final readonly class Key
{
    public function __construct(
        public string $column,
        public Order $order,
    ) {}

    public static function asc(string $column): self
    {
        return new self($column, Order::ASC);
    }

    public static function desc(string $column): self
    {
        return new self($column, Order::DESC);
    }

    public function toKeysetColumn(): KeysetColumn
    {
        return sql_keyset_column($this->column, $this->order->toSortOrder());
    }
}
