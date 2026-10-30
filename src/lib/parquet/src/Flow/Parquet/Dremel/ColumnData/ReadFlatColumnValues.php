<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\RowSelection;
use Generator;

use function array_values;
use function count;
use function is_array;
use function iterator_to_array;
use function sprintf;

final readonly class ReadFlatColumnValues
{
    private int $rowsCount;

    /**
     * @var list<mixed> the decoded values, kept so the page can be read again
     */
    private array $values;

    /**
     * @param iterable<mixed> $values
     * @param array<int> $repetitionLevels
     * @param array<int> $definitionLevels
     */
    public function __construct(
        public FlatColumn $column,
        iterable $values,
        private array $repetitionLevels,
        private array $definitionLevels,
    ) {
        $this->values = is_array($values) ? array_values($values) : iterator_to_array($values, false);

        $rowsCount = 0;

        foreach ($repetitionLevels as $repetitionLevel) {
            $rowsCount += $repetitionLevel === 0 ? 1 : 0;
        }

        $this->rowsCount = $rowsCount;
    }

    /**
     * @return \Generator<array-key, mixed>
     */
    public function assembleFlat(DataConverter $dataConverter): Generator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();
        $next = 0;

        foreach ($this->definitionLevels as $definitionLevel) {
            if ($definitionLevel === $maxDefinitionLevel) {
                // decoded parquet column values: any scalar/object the converter accepts
                // @mago-ignore analysis:mixed-assignment
                $value = $this->values[$next++] ?? null;

                yield $dataConverter->fromParquetType($this->column, $value);
            } else {
                yield new NullLevel($definitionLevel);
            }
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
        return $this->column->flatPath();
    }

    public function isEmpty(): bool
    {
        return !count($this->repetitionLevels) && !count($this->definitionLevels);
    }

    /**
     * @return \Generator<array-key, FlatValue>
     */
    public function iterator(): Generator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();
        $next = 0;

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionLevel) {
                /** @var null|scalar $value */
                $value = $this->values[$next++] ?? null;
            } else {
                $value = null;
            }

            yield new FlatValue($this->column, $this->repetitionLevels[$index], $definitionLevel, $value);
        }
    }

    /**
     * @return list<mixed>
     */
    public function rawValues(): array
    {
        return $this->values;
    }

    /**
     * @return array<int>
     */
    public function repetitionLevels(): array
    {
        return $this->repetitionLevels;
    }

    /**
     * The rows $rows selects, its row 0 being this page's first row; a row starts at each repetition level 0.
     */
    public function select(RowSelection $rows): self
    {
        if ($rows->totalRows() !== $this->rowsCount()) {
            throw new InvalidArgumentException(sprintf(
                'A selection of %d rows cannot select from a page of %d rows',
                $rows->totalRows(),
                $this->rowsCount(),
            ));
        }

        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();
        $selectors = $rows->selectors();
        $selector = -1;
        $remaining = 0;
        $skip = true;
        $repetitionLevels = [];
        $definitionLevels = [];
        $keptValues = [];
        $next = 0;

        foreach ($this->repetitionLevels as $index => $repetitionLevel) {
            if ($repetitionLevel === 0) {
                if ($remaining === 0) {
                    $remaining = $selectors[++$selector]->rowCount;
                }

                $remaining--;
                $skip = $selectors[$selector]->skip;
            }

            if ($this->definitionLevels[$index] === $maxDefinitionLevel) {
                if (!$skip) {
                    $keptValues[] = $this->values[$next] ?? null;
                }

                $next++;
            }

            if (!$skip) {
                $repetitionLevels[] = $repetitionLevel;
                $definitionLevels[] = $this->definitionLevels[$index];
            }
        }

        return new self($this->column, $keptValues, $repetitionLevels, $definitionLevels);
    }

    public function rowsCount(): int
    {
        return $this->rowsCount;
    }
}
