<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ReadFlatColumnValues
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
        private readonly array $definitionLevels = [],
        private array $values = [],
    ) {
    }

    /**
     * @return array<int>
     */
    public function definitionLevels() : array
    {
        return $this->definitionLevels;
    }

    public function flatPath() : string
    {
        return $this->column->flatPath();
    }

    public function isEmpty() : bool
    {
        return !\count($this->repetitionLevels) && !\count($this->definitionLevels);
    }

    /**
     * @return \Generator<array-key, FlatValue>
     */
    public function iterator() : \Generator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        $valueIndex = 0;

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            yield new FlatValue(
                $this->column,
                $this->repetitionLevels[$index],
                $definitionLevel,
                $definitionLevel === $maxDefinitionLevel ? $this->values[$valueIndex] : null
            );

            if ($definitionLevel === $maxDefinitionLevel) {
                $valueIndex++;
            }
        }
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
        $rowsCount = 0;

        foreach ($this->repetitionLevels as $repetitionLevel) {
            if ($repetitionLevel === 0) {
                $rowsCount++;
            }
        }

        return $rowsCount;
    }

    public function skipRows(?int $skipRows) : self
    {
        if ($skipRows === null || $skipRows <= 0) {
            return $this;
        }

        $chunk = [
            'repetitions' => [],
            'definitions' => [],
            'values' => [],
        ];

        $valueIndex = 0;
        $maxDefinitionsLevel = $this->column->maxDefinitionsLevel();

        $skippedRows = 0;
        $collect = false;

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionsLevel) {
                $value = $this->values[$valueIndex];
                $valueIndex++;
            } else {
                $value = null;
            }

            $repetitionLevel = $this->repetitionLevels[$index];

            if ($repetitionLevel === 0) {
                if ($skippedRows < $skipRows) {
                    $skippedRows++;

                    continue;
                }
                $collect = true;
            }

            if ($collect) {
                $chunk['repetitions'][] = $repetitionLevel;
                $chunk['definitions'][] = $definitionLevel;

                if ($value !== null) {
                    $chunk['values'][] = $value;
                }
            }
        }

        return new self($this->column, $chunk['repetitions'], $chunk['definitions'], $chunk['values']);
    }

    /**
     * @return array<null|scalar>
     */
    public function values() : array
    {
        return $this->values;
    }
}
