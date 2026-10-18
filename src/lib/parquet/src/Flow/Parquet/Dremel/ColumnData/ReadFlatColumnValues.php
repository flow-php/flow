<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\RowSelection;
use Generator;

use function count;
use function sprintf;

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
        private Generator $values,
        private array $repetitionLevels,
        private array $definitionLevels,
    ) {}

    /**
     * @return \Generator<array-key, mixed>
     */
    public function assembleFlat(DataConverter $dataConverter): Generator
    {
        $maxDefinitionLevel = $this->column->repetitions()->maxDefinitionLevel();

        foreach ($this->definitionLevels as $definitionLevel) {
            if ($definitionLevel === $maxDefinitionLevel) {
                // Iterator over decoded parquet column values (any scalar/object the converter accepts).
                // @mago-ignore analysis:mixed-assignment
                $value = $this->values->valid() ? $this->values->current() : null;
                $this->values->next();

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

        foreach ($this->definitionLevels as $index => $definitionLevel) {
            if ($definitionLevel === $maxDefinitionLevel) {
                /** @var null|scalar $value */
                $value = $this->values->valid() ? $this->values->current() : null;
                $this->values->next();
            } else {
                $value = null;
            }

            yield new FlatValue($this->column, $this->repetitionLevels[$index], $definitionLevel, $value);
        }
    }

    /**
     * @return \Generator<mixed>
     */
    public function rawValues(): Generator
    {
        yield from $this->values;
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

        foreach ($this->repetitionLevels as $index => $repetitionLevel) {
            if ($repetitionLevel === 0) {
                if ($remaining === 0) {
                    $remaining = $selectors[++$selector]->rowCount;
                }

                $remaining--;
                $skip = $selectors[$selector]->skip;
            }

            if ($this->definitionLevels[$index] === $maxDefinitionLevel) {
                $keptValues[] = !$skip;
            }

            if (!$skip) {
                $repetitionLevels[] = $repetitionLevel;
                $definitionLevels[] = $this->definitionLevels[$index];
            }
        }

        $values = $this->values;

        return new self(
            $this->column,
            (static function () use ($values, $keptValues): Generator {
                foreach ($keptValues as $kept) {
                    if ($kept) {
                        yield $values->current();
                    }

                    $values->next();
                }
            })(),
            $repetitionLevels,
            $definitionLevels,
        );
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
}
