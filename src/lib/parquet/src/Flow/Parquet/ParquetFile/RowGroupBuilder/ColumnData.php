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

    public function iterator(FlatColumn $column) : \Iterator
    {
        return $this->children[$column->flatPath()]->iterator();
    }

    public function merge(self $columnData) : self
    {
        foreach ($columnData->children as $data) {
            $this->children[$data->column->flatPath()]->merge($data);
        }

        return $this;
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

    public function values(string $flatPath) : FlatColumnValues
    {
        return $this->children[$flatPath];
    }
}
