<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\{FlatValue, ReadFlatColumnValues};
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class ReadColumnData
{
    public Column $column;

    /**
     * @var array<string, ReadFlatColumnValues>
     */
    public array $flatValues;

    /**
     * @param Column $column
     * @param array<ReadFlatColumnValues> $flatValues
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

    /**
     * @return \Iterator<array-key, FlatValue>
     */
    public function iterator(FlatColumn $column) : \Iterator
    {
        return $this->flatValues[$column->flatPath()]->iterator();
    }
}
