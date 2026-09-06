<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Generator;
use IteratorAggregate;

/**
 * An IteratorAggregate that hands out a FRESH generator on every getIterator() call, so it reads like a
 * rewindable source while being a Traversable rather than an array - the shape no rewindability
 * predicate can tell apart from the aggregate that re-yields one exhausted generator.
 *
 * @implements IteratorAggregate<int, array<string, mixed>>
 */
final class FreshRowsAggregate implements IteratorAggregate
{
    /**
     * @param list<array<string, mixed>> $rows
     */
    public function __construct(
        private readonly array $rows,
    ) {}

    public function getIterator(): Generator
    {
        yield from $this->rows;
    }
}
