<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatColumnValues, FlatValue};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final class ColumnData
{
    /**
     * @param Column $column
     * @param array<FlatColumnValues> $children
     */
    private function __construct(public readonly Column $column, private readonly array $children = [])
    {
    }

    /**
     * @param Column $column
     * @param array<array-key, FlatColumnValues> $children
     */
    public static function from(Column $column, array $children) : self
    {
        $childrenPaths = [];

        foreach ($children as $child) {
            $childrenPaths[] = $child->column->flatPath();
        }

        $columnPaths = [];

        if ($column instanceof FlatColumn) {
            $columnPaths[] = $column->flatPath();
        }

        if ($column instanceof NestedColumn) {
            foreach ($column->childrenFlat() as $columnChild) {
                $columnPaths[] = $columnChild->flatPath();
            }
        }

        \sort($childrenPaths);
        \sort($columnPaths);

        if ($childrenPaths !== $columnPaths) {
            throw new RuntimeException('ColumnData children must have the same paths as the column');
        }

        return new self($column, $children);
    }

    public static function initialize(Column $column) : self
    {
        $children = [];

        if ($column instanceof FlatColumn) {
            $children[$column->flatPath()] = new FlatColumnValues($column);
        }

        if ($column instanceof NestedColumn) {
            foreach ($column->childrenFlat() as $columnChild) {
                $children[$columnChild->flatPath()] = new FlatColumnValues($columnChild);
            }
        }

        return new self($column, $children);
    }

    public function add(FlatValue ...$values) : void
    {
        foreach ($values as $cell) {
            $this->children[$cell->column->flatPath()]->add($cell);
        }
    }

    public function isEmpty(FlatColumn $column) : bool
    {
        foreach ($this->children as $child) {
            if ($child->column->flatPath() === $column->flatPath()) {
                return $child->isEmpty();
            }
        }

        throw new RuntimeException('Column ' . $column->flatPath() . ' not found in FlatData');
    }

    /**
     * @return \ArrayIterator<array-key, FlatValue>
     */
    public function iterator() : \ArrayIterator
    {
        $iterator = new \MultipleIterator(\MultipleIterator::MIT_NEED_ALL);

        foreach ($this->children as $child) {
            $iterator->attachIterator($child->iterator());
        }

        /**
         * @var array<FlatValue> $values
         */
        $values = [];

        foreach ($iterator as $val) {
            $values[] = $val;
        }

        return new \ArrayIterator($values);
    }

    public function merge(FlatColumnValues ...$flatData) : void
    {
        foreach ($flatData as $data) {
            $this->children[$data->column->flatPath()]->merge($data);
        }
    }

    /**
     * @return array<string, {repetition_levels: array<int>, definition_levels: array<int>, values: array<mixed>}>
     */
    public function normalize() : array
    {
        $normalized = [];

        foreach ($this->children as $child) {
            $normalized[$child->column->flatPath()] = [
                'repetition_levels' => $child->repetitionLevels(),
                'definition_levels' => $child->definitionLevels(),
                'values' => $child->values(),
            ];
        }

        return $normalized;
    }
}
