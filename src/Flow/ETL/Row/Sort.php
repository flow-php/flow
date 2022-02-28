<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @psalm-immutable
 */
final class Sort
{
    private string $column;

    private string $order;

    private function __construct(string $column, string $order)
    {
        $this->column = $column;
        $this->order = $order;
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
