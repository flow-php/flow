<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatColumnValues, FlatValue};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final class FlatColumnData
{
    /**
     * @param Column $column
     * @param array<string, FlatColumnValues> $flatValues
     */
    private function __construct(public readonly Column $column, private readonly array $flatValues = [])
    {
    }

    public static function initialize(Column $column) : self
    {
        $flatValues = [];

        if ($column instanceof FlatColumn) {
            $flatValues[$column->flatPath()] = new FlatColumnValues($column);
        }

        if ($column instanceof NestedColumn) {
            foreach ($column->childrenFlat() as $columnChild) {
                $flatValues[$columnChild->flatPath()] = new FlatColumnValues($columnChild);
            }
        }

        return new self($column, $flatValues);
    }

    public function addValue(FlatValue ...$values) : void
    {
        foreach ($values as $cell) {
            $this->flatValues[$cell->column->flatPath()]->add($cell);
        }
    }

    public function addValues(FlatColumnValues $values) : void
    {
        $this->flatValues[$values->column->flatPath()]->merge($values);
    }

    /**
     * @return array<string, FlatColumnValues>
     */
    public function flatValues() : array
    {
        return $this->flatValues;
    }

    public function isEmpty(FlatColumn $column) : bool
    {
        foreach ($this->flatValues as $child) {
            if ($child->column->flatPath() === $column->flatPath()) {
                return $child->isEmpty();
            }
        }

        throw new RuntimeException('Column ' . $column->flatPath() . ' not found in FlatData');
    }

    /**
     * @return \Iterator<array-key, FlatValue>
     */
    public function iterator(FlatColumn $column) : \Iterator
    {
        return $this->flatValues[$column->flatPath()]->iterator();
    }

    public function merge(self $columnData) : self
    {
        foreach ($columnData->flatValues as $data) {
            $this->flatValues[$data->column->flatPath()]->merge($data);
        }

        return $this;
    }

    /**
     * @return array<string, array{repetition_levels: array<int>, definition_levels: array<int>, values: array<mixed>}>
     */
    public function normalize() : array
    {
        $normalized = [];

        foreach ($this->flatValues as $child) {
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
        return $this->flatValues[$flatPath];
    }
}
