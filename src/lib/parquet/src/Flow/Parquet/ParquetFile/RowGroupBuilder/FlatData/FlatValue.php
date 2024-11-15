<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\FlatData;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class FlatValue
{
    public function __construct(
        public FlatColumn $column,
        private array $repetitionLevels,
        private array $definitionLevels,
        private array $values,
    ) {
    }

    public function definitionLevels() : array
    {
        return $this->definitionLevels;
    }

    public function isEmpty() : bool
    {
        return !\count($this->values) && !\count($this->repetitionLevels) && !\count($this->definitionLevels);
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

    public function repetitionLevels() : array
    {
        return $this->repetitionLevels;
    }

    public function values() : array
    {
        return $this->values;
    }
}
