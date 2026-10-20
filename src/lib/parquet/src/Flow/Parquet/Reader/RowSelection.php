<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;

use function array_key_last;
use function array_pop;
use function array_sum;
use function count;
use function min;
use function sprintf;

/**
 * File-absolute rows: the selectors cover every row of the file, in order, adjacent selectors merged.
 */
final readonly class RowSelection
{
    /**
     * @var list<RowSelector>
     */
    private array $selectors;

    /**
     * @var list<int> the first row of each selector
     */
    private array $starts;

    public function __construct(RowSelector ...$selectors)
    {
        /** @var list<RowSelector> $merged */
        $merged = [];

        foreach ($selectors as $selector) {
            if ($selector->rowCount === 0) {
                continue;
            }

            $previous = array_pop($merged);

            if ($previous !== null && $previous->skip === $selector->skip) {
                $merged[] = new RowSelector($previous->rowCount + $selector->rowCount, $selector->skip);

                continue;
            }

            if ($previous !== null) {
                $merged[] = $previous;
            }

            $merged[] = $selector;
        }

        $starts = [];
        $row = 0;

        foreach ($merged as $selector) {
            $starts[] = $row;
            $row += $selector->rowCount;
        }

        $this->selectors = $merged;
        $this->starts = $starts;
    }

    public static function all(int $rows): self
    {
        return new self(new RowSelector($rows, false));
    }

    public static function none(int $rows): self
    {
        return new self(new RowSelector($rows, true));
    }

    /**
     * The rows from $offset on, $limit of them at most.
     */
    public static function window(int $rows, int $offset = 0, ?int $limit = null): self
    {
        $offset = min($offset, $rows);
        $selected = min($limit ?? ($rows - $offset), $rows - $offset);

        return new self(
            new RowSelector($offset, true),
            new RowSelector($selected, false),
            new RowSelector($rows - $offset - $selected, true),
        );
    }

    /**
     * Whether every row the other selection selects is selected here.
     */
    public function contains(self $other): bool
    {
        return $other->intersection($this)->rowCount() === $other->rowCount();
    }

    public function difference(self $other): self
    {
        return $this->combine($other, static fn(bool $leftSkips, bool $rightSkips): bool => $leftSkips || !$rightSkips);
    }

    public function head(int $rows): self
    {
        $selectors = [];

        foreach ($this->selectors as $selector) {
            $taken = $selector->skip ? 0 : min($rows, $selector->rowCount);
            $selectors[] = new RowSelector($taken, false);
            $selectors[] = new RowSelector($selector->rowCount - $taken, true);
            $rows -= $taken;
        }

        return new self(...$selectors);
    }

    public function intersection(self $other): self
    {
        return $this->combine($other, static fn(bool $leftSkips, bool $rightSkips): bool => $leftSkips || $rightSkips);
    }

    /**
     * The selected rows whose entry is true, one entry per selected row in order.
     *
     * @param list<bool> $mask
     */
    public function masked(array $mask): self
    {
        if (count($mask) !== $this->rowCount()) {
            throw new InvalidArgumentException(sprintf(
                'A mask of %d rows cannot narrow a selection of %d rows',
                count($mask),
                $this->rowCount(),
            ));
        }

        $selectors = [];
        $row = 0;

        foreach ($this->selectors as $selector) {
            if ($selector->skip) {
                $selectors[] = $selector;

                continue;
            }

            $run = 0;
            $skip = !$mask[$row];

            for ($end = $row + $selector->rowCount; $row < $end; $row++) {
                if (!$mask[$row] !== $skip) {
                    $selectors[] = new RowSelector($run, $skip);
                    $run = 0;
                    $skip = !$skip;
                }

                $run++;
            }

            $selectors[] = new RowSelector($run, $skip);
        }

        return new self(...$selectors);
    }

    public function union(self $other): self
    {
        return $this->combine($other, static fn(bool $leftSkips, bool $rightSkips): bool => $leftSkips && $rightSkips);
    }

    /**
     * Rows selected.
     *
     * @return non-negative-int
     */
    public function rowCount(): int
    {
        $rows = 0;

        foreach ($this->selectors as $selector) {
            $rows += $selector->skip ? 0 : $selector->rowCount;
        }

        return $rows;
    }

    /**
     * @return list<RowSelector>
     */
    public function selectors(): array
    {
        return $this->selectors;
    }

    /**
     * $rows rows from row $offset on, row 0 being row $offset.
     */
    public function slice(int $offset, int $rows): self
    {
        if ($offset < 0 || $rows < 0 || ($offset + $rows) > $this->totalRows()) {
            throw new InvalidArgumentException(sprintf(
                'Rows %d to %d lie outside a selection of %d rows',
                $offset,
                $offset + $rows,
                $this->totalRows(),
            ));
        }

        $low = 0;
        $high = count($this->starts) - 1;

        while ($low < $high) {
            $middle = ($low + $high + 1) >> 1;

            if ($this->starts[$middle] <= $offset) {
                $low = $middle;
            } else {
                $high = $middle - 1;
            }
        }

        $selectors = [];

        for ($index = $low, $row = $offset, $end = $offset + $rows; $row < $end; $index++) {
            $taken = min($end, $this->starts[$index] + $this->selectors[$index]->rowCount) - $row;
            $selectors[] = new RowSelector($taken, $this->selectors[$index]->skip);
            $row += $taken;
        }

        return new self(...$selectors);
    }

    /**
     * One selection per consecutive part of the given row counts, row 0 of each being the part's first row.
     *
     * @param list<int> $rowCounts
     *
     * @return list<self>
     */
    public function split(array $rowCounts): array
    {
        if (array_sum($rowCounts) !== $this->totalRows()) {
            throw new InvalidArgumentException(sprintf(
                'A selection of %d rows cannot be split into parts of %d rows',
                $this->totalRows(),
                array_sum($rowCounts),
            ));
        }

        $parts = [];
        $offset = 0;

        foreach ($rowCounts as $rowCount) {
            $parts[] = $this->slice($offset, $rowCount);
            $offset += $rowCount;
        }

        return $parts;
    }

    public function selectsAny(): bool
    {
        foreach ($this->selectors as $selector) {
            if (!$selector->skip) {
                return true;
            }
        }

        return false;
    }

    /**
     * Rows covered, selected or skipped.
     */
    public function totalRows(): int
    {
        $last = array_key_last($this->selectors);

        return $last === null ? 0 : $this->starts[$last] + $this->selectors[$last]->rowCount;
    }

    /**
     * @param callable(bool, bool): bool $skip whether a row is skipped, from whether each selection skips it
     */
    private function combine(self $other, callable $skip): self
    {
        if ($this->totalRows() !== $other->totalRows()) {
            throw new InvalidArgumentException(sprintf(
                'Row selections of %d and %d rows cannot be combined',
                $this->totalRows(),
                $other->totalRows(),
            ));
        }

        $left = $this->selectors;
        $right = $other->selectors;

        if ($left === []) {
            return new self();
        }

        $combined = [];
        $i = 0;
        $j = 0;
        $leftRemaining = $left[0]->rowCount;
        $rightRemaining = $right[0]->rowCount;

        while ($i < count($left) && $j < count($right)) {
            $rows = min($leftRemaining, $rightRemaining);
            $combined[] = new RowSelector($rows, $skip($left[$i]->skip, $right[$j]->skip));
            $leftRemaining -= $rows;
            $rightRemaining -= $rows;

            if ($leftRemaining === 0 && ++$i < count($left)) {
                $leftRemaining = $left[$i]->rowCount;
            }

            if ($rightRemaining === 0 && ++$j < count($right)) {
                $rightRemaining = $right[$j]->rowCount;
            }
        }

        return new self(...$combined);
    }
}
