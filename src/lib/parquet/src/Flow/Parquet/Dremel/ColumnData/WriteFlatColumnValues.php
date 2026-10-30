<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Generator;

use function assert;
use function count;
use function gettype;
use function is_scalar;

final class WriteFlatColumnValues
{
    private readonly string $flatPath;

    /**
     * @param FlatColumn $column
     * @param array<int> $repetitionLevels
     * @param array<int> $definitionLevels
     * @param array<null|object|scalar> $values
     */
    public function __construct(
        public readonly FlatColumn $column,
        public array $repetitionLevels = [],
        public array $definitionLevels = [],
        public array $values = [],
    ) {
        $this->flatPath = $column->flatPath();
    }

    public function add(FlatValue $cell): void
    {
        assert(
            $cell->column->flatPath() === $this->flatPath,
            'Cannot add data from different column, attempt to merge: ' . $this->flatPath . ' with '
                . $cell->column->flatPath(),
        );

        $this->repetitionLevels[] = $cell->repetitionLevel;
        $this->definitionLevels[] = $cell->definitionLevel;

        if ($cell->value !== null) {
            $this->values[] = $cell->value;
        }
    }

    /**
     * @return array<int>
     */
    public function definitionLevels(): array
    {
        return $this->definitionLevels;
    }

    public function flatPath(): string
    {
        return $this->flatPath;
    }

    public function isEmpty(): bool
    {
        return !count($this->values) && !count($this->repetitionLevels) && !count($this->definitionLevels);
    }

    /**
     * @return \Generator<FlatValue>
     */
    public function iterator(): Generator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        $valueIndex = 0;

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            $rawValue = $definitionLevel === $maxDefinitionLevel ? $this->values[$valueIndex] : null;

            if ($rawValue !== null && !is_scalar($rawValue)) {
                throw new InvalidArgumentException(
                    'FlatValue iterator requires scalar values, got ' . gettype($rawValue),
                );
            }

            yield new FlatValue($this->column, $this->repetitionLevels[$index], $definitionLevel, $rawValue);

            if ($definitionLevel === $maxDefinitionLevel) {
                $valueIndex++;
            }
        }
    }

    public function merge(self $flatData): self
    {
        assert(
            $flatData->flatPath === $this->flatPath,
            'Cannot merge different column, attempt to merge: ' . $this->flatPath . ' with ' . $flatData->flatPath,
        );

        array_push($this->repetitionLevels, ...$flatData->repetitionLevels);
        array_push($this->definitionLevels, ...$flatData->definitionLevels);
        array_push($this->values, ...$flatData->values);

        return $this;
    }

    public function nullCount(): int
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
    public function repetitionLevels(): array
    {
        return $this->repetitionLevels;
    }

    public function rowsCount(): int
    {
        $rowsCount = 0;

        foreach ($this->repetitionLevels as $repetitionLevel) {
            if ($repetitionLevel === 0) {
                $rowsCount++;
            }
        }

        return $rowsCount;
    }

    /**
     * @return array<null|object|scalar>
     */
    public function values(): array
    {
        return $this->values;
    }
}
