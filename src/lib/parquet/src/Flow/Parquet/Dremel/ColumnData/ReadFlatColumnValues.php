<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class ReadFlatColumnValues
{
    /**
     * @param FlatColumn $column
     * @param \Generator<mixed, mixed, mixed, mixed> $values
     * @param array<int> $repetitionLevels
     * @param array<int> $definitionLevels
     */
    public function __construct(
        public FlatColumn $column,
        private \Generator $values,
        private array $repetitionLevels,
        private array $definitionLevels,
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

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionLevel) {
                /** @var null|scalar $value */
                $value = $this->values->valid() ? $this->values->current() : null;
                $this->values->next();
            } else {
                $value = null;
            }

            yield new FlatValue(
                $this->column,
                $this->repetitionLevels[$index],
                $definitionLevel,
                $value
            );
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
}
