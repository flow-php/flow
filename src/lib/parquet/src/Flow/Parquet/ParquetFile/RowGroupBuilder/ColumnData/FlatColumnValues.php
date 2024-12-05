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
     * @psalm-suppress InvalidReturnStatement
     * @psalm-suppress InvalidReturnType
     *
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

        $this->repetitionLevels = [...$this->repetitionLevels, ...$flatData->repetitionLevels];
        $this->definitionLevels = [...$this->definitionLevels, ...$flatData->definitionLevels];
        $this->values = [...$this->values, ...$flatData->values];

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

    public function skipRows(?int $skipRows) : self
    {
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

            if ($skippedRows >= $skipRows && $repetitionLevel === 0) {
                $collect = true;
            }

            if ($repetitionLevel === 0 && $collect === false) {
                $skippedRows++;

                continue;
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
     * @return array<FlatColumnValues>
     */
    public function splitByRows(int $rowsInChunk) : array
    {
        $rows = [];
        $rowsChunkData = [
            'repetitions' => [],
            'definitions' => [],
            'values' => [],
        ];

        $valueIndex = 0;
        $maxDefinitionsLevel = $this->column->maxDefinitionsLevel();

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionsLevel) {
                $value = $this->values[$valueIndex];
                $valueIndex++;
            } else {
                $value = null;
            }

            $repetitionLevel = $this->repetitionLevels[$index];

            if ($repetitionLevel === 0 && \count($rowsChunkData['repetitions']) >= $rowsInChunk) {
                $rows[] = new self($this->column, $rowsChunkData['repetitions'], $rowsChunkData['definitions'], $rowsChunkData['values']);
                $rowsChunkData['repetitions'] = [];
                $rowsChunkData['definitions'] = [];
                $rowsChunkData['values'] = [];
            }

            $rowsChunkData['repetitions'][] = $repetitionLevel;
            $rowsChunkData['definitions'][] = $definitionLevel;

            if ($value !== null) {
                $rowsChunkData['values'][] = $value;
            }
        }

        if (\count($rowsChunkData['repetitions']) > 0) {
            $rows[] = new self($this->column, $rowsChunkData['repetitions'], $rowsChunkData['definitions'], $rowsChunkData['values']);
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
