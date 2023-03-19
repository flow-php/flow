<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

final class Sort
{
    private function __construct(
        private readonly string $column,
        private readonly string $order
    ) {
    }

    public static function asc(string $column) : self
    {
        return new self($column, 'asc');
    }

    public static function desc(string $column) : self
    {
        return new self($column, 'desc');
    }

    public function isAsc() : bool
    {
        return $this->order === 'asc';
    }

    public function name() : string
    {
        return $this->column;
    }
}
