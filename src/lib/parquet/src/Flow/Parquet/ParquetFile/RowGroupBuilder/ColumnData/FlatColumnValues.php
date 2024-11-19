<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class FlatColumnValues
{
    /**
     * @param FlatColumn $column
     * @param array<int> $repetitionLevels
     * @param array<int> $definitionLevels
     * @param array<null|scalar> $values
     */
    public function __construct(
        public readonly FlatColumn $column,
        private array $repetitionLevels = [],
        private array $definitionLevels = [],
        private array $values = [],
    ) {
    }

    public function add(FlatValue $cell) : void
    {
        if ($cell->column->flatPath() !== $this->column->flatPath()) {
            throw new RuntimeException('Cannot add data from different column, attempt to merge: ' . $this->column->flatPath() . ' with ' . $cell->column->flatPath());
        }

        $this->repetitionLevels[] = $cell->repetitionLevel;
        $this->definitionLevels[] = $cell->definitionLevel;

        if ($cell->value !== null) {
            $this->values[] = $cell->value;
        }
    }

    /**
     * @return array<int>
     */
    public function definitionLevels() : array
    {
        return $this->definitionLevels;
    }

    public function isEmpty() : bool
    {
        return !\count($this->values) && !\count($this->repetitionLevels) && !\count($this->definitionLevels);
    }

    /**
     * @return \ArrayIterator<array-key, FlatValue>
     */
    public function iterator() : \ArrayIterator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        $valueIndex = 0;
        $values = [];

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            $values[] = new FlatValue(
                $this->column,
                $this->repetitionLevels[$index],
                $definitionLevel,
                $definitionLevel === $maxDefinitionLevel ? $this->values[$valueIndex] : null
            );

            $valueIndex++;
        }

        return new \ArrayIterator($values);
    }

    public function merge(self $flatData) : void
    {
        if ($flatData->column->flatPath() !== $this->column->flatPath()) {
            throw new RuntimeException('Cannot merge different column, attempt to merge: ' . $this->column->flatPath() . ' with ' . $flatData->column->flatPath());
        }

        $this->repetitionLevels = array_merge($this->repetitionLevels, $flatData->repetitionLevels);
        $this->definitionLevels = array_merge($this->definitionLevels, $flatData->definitionLevels);
        $this->values = array_merge($this->values, $flatData->values);
    }

    /**
     * @return array<int>
     */
    public function repetitionLevels() : array
    {
        return $this->repetitionLevels;
    }

    /**
     * @return array<null|scalar>
     */
    public function values() : array
    {
        return $this->values;
    }
}
