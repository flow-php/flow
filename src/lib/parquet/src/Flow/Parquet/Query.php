<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Filter;

use function array_unique;
use function array_values;
use function count;

/**
 * The rows a read returns: the selected columns (all root columns when none) of the rows matching the filter, the
 * offset and the limit counted over the matching rows.
 */
final readonly class Query
{
    /**
     * @var list<string>
     */
    public array $columns;

    /**
     * @param array<string> $columns
     */
    public function __construct(
        array $columns = [],
        public ?Filter $filter = null,
        public int $offset = 0,
        public ?int $limit = null,
    ) {
        foreach ($columns as $column) {
            if ($column === '') {
                throw new InvalidArgumentException('A query column name cannot be empty');
            }
        }

        if (count(array_unique($columns)) !== count($columns)) {
            throw new InvalidArgumentException('A query selects each column once');
        }

        if ($offset < 0) {
            throw new InvalidArgumentException('Query offset must be 0 or more, got ' . $offset);
        }

        if ($limit !== null && $limit < 0) {
            throw new InvalidArgumentException('Query limit must be 0 or more, got ' . $limit);
        }

        $this->columns = array_values($columns);
    }

    public function limit(int $limit): self
    {
        return new self($this->columns, $this->filter, $this->offset, $limit);
    }

    public function offset(int $offset): self
    {
        return new self($this->columns, $this->filter, $offset, $this->limit);
    }

    public function select(string ...$columns): self
    {
        if ($columns === []) {
            throw new InvalidArgumentException('A query selects at least one column');
        }

        return new self(array_values($columns), $this->filter, $this->offset, $this->limit);
    }

    /**
     * A second filter narrows the first: both must match.
     */
    public function where(Filter $filter): self
    {
        return new self(
            $this->columns,
            match (true) {
                $this->filter === null => $filter,
                $this->filter instanceof All => new All(...$this->filter->filters, ...[$filter]),
                default => new All($this->filter, $filter),
            },
            $this->offset,
            $this->limit,
        );
    }
}
