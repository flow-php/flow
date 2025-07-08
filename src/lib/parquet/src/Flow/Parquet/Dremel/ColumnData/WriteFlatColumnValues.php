<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class WriteFlatColumnValues
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
     * @return \Generator<FlatValue>
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

    public function merge(self $flatData) : self
    {
        if ($flatData->column->flatPath() !== $this->column->flatPath()) {
            throw new RuntimeException('Cannot merge different column, attempt to merge: ' . $this->column->flatPath() . ' with ' . $flatData->column->flatPath());
        }

        array_push($this->repetitionLevels, ...$flatData->repetitionLevels);
        array_push($this->definitionLevels, ...$flatData->definitionLevels);
        array_push($this->values, ...$flatData->values);

        return $this;
    }

    public function nullCount() : int
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        $nullCount = 0;

        foreach ($this->definitionLevels as $definitionLevel) {
            if ($definitionLevel !== $maxDefinitionLevel) {
                $nullCount++;
            }
        }

        return $nullCount;
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
     * @param int $rowsInChunk
     *
     * @return array<WriteFlatColumnValues>
     */
    public function splitByRows(int $rowsInChunk) : array
    {
        $chunks = [];
        $currentChunk = [
            'repetitions' => [],
            'definitions' => [],
            'values' => [],
        ];

        $valueIndex = 0;
        $maxDefinitionsLevel = $this->column->maxDefinitionsLevel();
        $rowsInCurrentChunk = 0;
        $pageBreakIndexes = [];

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionsLevel) {
                $value = $this->values[$valueIndex];
                $valueIndex++;
            } else {
                $value = null;
            }

            $repetitionLevel = $this->repetitionLevels[$index];

            if ($repetitionLevel === 0 && $rowsInCurrentChunk >= $rowsInChunk && \count($currentChunk['repetitions']) > 0) {
                $pageBreakIndexes[] = $index;
                $chunks[] = new self($this->column, $currentChunk['repetitions'], $currentChunk['definitions'], $currentChunk['values']);
                $currentChunk = [
                    'repetitions' => [],
                    'definitions' => [],
                    'values' => [],
                ];
                $rowsInCurrentChunk = 0;
            }

            $currentChunk['repetitions'][] = $repetitionLevel;
            $currentChunk['definitions'][] = $definitionLevel;

            if ($value !== null) {
                $currentChunk['values'][] = $value;
            }

            if ($repetitionLevel === 0) {
                $rowsInCurrentChunk++;
            }
        }

        if (\count($currentChunk['repetitions']) > 0) {
            $chunks[] = new self($this->column, $currentChunk['repetitions'], $currentChunk['definitions'], $currentChunk['values']);
        }

        return $chunks;
    }

    /**
     * @return array<null|scalar>
     */
    public function values() : array
    {
        return $this->values;
    }
}
