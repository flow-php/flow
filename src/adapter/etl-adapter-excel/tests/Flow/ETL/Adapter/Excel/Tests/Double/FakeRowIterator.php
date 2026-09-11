<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Double;

use ArrayIterator;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Reader\RowIteratorInterface;

final class FakeRowIterator implements RowIteratorInterface
{
    /**
     * @param ArrayIterator<int, Row> $rows
     */
    public function __construct(
        private readonly ArrayIterator $rows,
    ) {}

    public function current(): ?Row
    {
        return $this->rows->valid() ? $this->rows->current() : null;
    }

    public function key(): int
    {
        return $this->rows->key();
    }

    public function next(): void
    {
        $this->rows->next();
    }

    public function rewind(): void
    {
        $this->rows->rewind();
    }

    public function valid(): bool
    {
        return $this->rows->valid();
    }
}
