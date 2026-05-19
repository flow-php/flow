<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Countable;
use Generator;
use IteratorAggregate;

/**
 * Cursor for lazy iteration over large result sets.
 * Use Client::cursor() to obtain a cursor.
 *
 * Supports both raw array iteration and object mapping via RowMapper.
 *
 * @extends \IteratorAggregate<int, array<string, mixed>>
 */
interface Cursor extends Countable, IteratorAggregate
{
    /**
     * Get the number of rows in the result set.
     */
    public function count(): int;

    /**
     * Free the cursor resources.
     * Called automatically when iteration completes.
     */
    public function free(): void;

    /**
     * Iterate all remaining rows lazily as arrays.
     * Memory efficient - one row at a time.
     *
     * @return \Generator<int, array<string, mixed>>
     */
    public function iterate(): Generator;

    /**
     * Iterate all remaining rows, mapping each using the provided mapper.
     * Memory efficient - one result at a time.
     *
     * @template T
     *
     * @param RowMapper<T> $mapper Mapper to apply to each row
     *
     * @return \Generator<int, T>
     */
    public function map(RowMapper $mapper): Generator;

    /**
     * Fetch the next row. Returns null when exhausted.
     *
     * @return null|array<string, mixed>
     */
    public function next(): ?array;
}
