<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatValue, ReadFlatColumnValues};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class ReadFlatColumnData
{
    /**
     * @param Column $column
     * @param array<string, ReadFlatColumnValues> $flatValues
     */
    public function __construct(public Column $column, public array $flatValues = [])
    {
    }

    public static function initialize(Column $column) : self
    {
        $flatValues = [];

        if ($column instanceof FlatColumn) {
            $flatValues[$column->flatPath()] = new ReadFlatColumnValues($column);
        }

        if ($column instanceof NestedColumn) {
            foreach ($column->childrenFlat() as $columnChild) {
                $flatValues[$columnChild->flatPath()] = new ReadFlatColumnValues($columnChild);
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

    public function addValues(ReadFlatColumnValues $values) : void
    {
        $this->flatValues[$values->column->flatPath()]->merge($values);
    }

    /**
     * @return array<string, ReadFlatColumnValues>
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

    public function values(string $flatPath) : ReadFlatColumnValues
    {
        return $this->flatValues[$flatPath];
    }
}
