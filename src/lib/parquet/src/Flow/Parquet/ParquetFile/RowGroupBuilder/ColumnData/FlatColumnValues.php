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

    public function definitionLevelsCount() : int
    {
        return \count($this->definitionLevels);
    }

    public function flatPath() : string
    {
        return $this->column->flatPath();
    }

    public function isEmpty() : bool
    {
        return !\count($this->values) && !\count($this->repetitionLevels) && !\count($this->definitionLevels);
    }

    /**
     * @return \ArrayIterator<int<0, max>, FlatValue>
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

            if ($definitionLevel === $maxDefinitionLevel) {
                $valueIndex++;
            }
        }

        return new \ArrayIterator($values);
    }

    public function merge(self $flatData) : self
    {
        if ($flatData->column->flatPath() !== $this->column->flatPath()) {
            throw new RuntimeException('Cannot merge different column, attempt to merge: ' . $this->column->flatPath() . ' with ' . $flatData->column->flatPath());
        }

        $this->repetitionLevels = array_merge($this->repetitionLevels, $flatData->repetitionLevels);
        $this->definitionLevels = array_merge($this->definitionLevels, $flatData->definitionLevels);
        $this->values = array_merge($this->values, $flatData->values);

        return $this;
    }

    public function nullCount() : int
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        return \count(\array_filter($this->definitionLevels, fn (int $d) => $d !== $maxDefinitionLevel));
    }

    /**
     * @return array<int>
     */
    public function repetitionLevels() : array
    {
        return $this->repetitionLevels;
    }

    public function rowsCount() : int
    {
        // rows count is count of repetitions equal to 0
        return \count(\array_filter($this->repetitionLevels, static fn (int $r) => $r === 0));
    }

    /**
     * @param int $rowsInChunk
     *
     * @return array<FlatColumnValues>
     */
    public function splitByRows(int $rowsInChunk) : array
    {
        $rows = [];

        $iterator = new \MultipleIterator(\MultipleIterator::MIT_NEED_ALL | \MultipleIterator::MIT_KEYS_ASSOC);

        $iterator->attachIterator(new \ArrayIterator($this->repetitionLevels), 'r');
        $iterator->attachIterator(new \ArrayIterator($this->definitionLevels), 'd');

        $row = new self($this->column);

        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();
        $valueIndex = 0;

        foreach ($iterator as $value) {
            if ($value['r'] === 0 && $row->rowsCount() === $rowsInChunk) {
                $rows[] = $row;
                $row = new self($this->column);
            }

            if ($value['d'] === $maxDefinitionLevel) {
                $row->add(new FlatValue($this->column, $value['r'], $value['d'], $this->values[$valueIndex]));
                $valueIndex++;
            } else {
                $row->add(new FlatValue($this->column, $value['r'], $value['d'], null));
            }
        }

        if ($row->isEmpty() === false) {
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * @return array<null|scalar>
     */
    public function values() : array
    {
        return $this->values;
    }
}
