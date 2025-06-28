<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatValue, ReadFlatColumnValues};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class ReadFlatColumnData
{
    public Column $column;

    /**
     * @var array<string, ReadFlatColumnValues>
     */
    private array $flatValues;

    /**
     * @param Column $column
     * @param array<string, ReadFlatColumnValues> $flatValues
     */
    public function __construct(Column $column, array $flatValues = [])
    {
        $indexedFlatValues = [];

        foreach ($flatValues as $flatValue) {
            $indexedFlatValues[$flatValue->column->flatPath()] = $flatValue;
        }

        if ($column instanceof FlatColumn) {
            if (!\array_key_exists($column->flatPath(), $indexedFlatValues)) {
                throw new InvalidArgumentException("Flat column '{$column->flatPath()}' is missing in flat values.");
            }
        }

        if ($column instanceof NestedColumn) {
            foreach ($column->childrenFlat() as $columnChild) {
                if (!\array_key_exists($columnChild->flatPath(), $indexedFlatValues)) {
                    throw new InvalidArgumentException("Flat column '{$columnChild->flatPath()}' is missing in flat values.");
                }
            }
        }

        $this->column = $column;
        $this->flatValues = $indexedFlatValues;
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
     * @return \Iterator<array-key, FlatValue>
     */
    public function iterator(FlatColumn $column) : \Iterator
    {
        return $this->flatValues[$column->flatPath()]->iterator();
    }
}
