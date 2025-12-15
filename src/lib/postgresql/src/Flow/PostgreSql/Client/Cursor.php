<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

/**
 * Cursor for lazy iteration over large result sets.
 * Use Client::cursor() to obtain a cursor.
 *
 * Supports both raw array iteration and object mapping via RowMapper.
 *
 * @extends \IteratorAggregate<int, array<string, mixed>>
 */
interface Cursor extends \Countable, \IteratorAggregate
{
    /**
     * Get the number of rows in the result set.
     */
    public function count() : int;

    /**
     * Free the cursor resources.
     * Called automatically when iteration completes.
     */
    public function free() : void;

    /**
     * Iterate all remaining rows lazily as arrays.
     * Memory efficient - one row at a time.
     *
     * @return \Generator<int, array<string, mixed>>
     */
    public function iterate() : \Generator;

    /**
     * Iterate all remaining rows, mapping each to an object.
     * Memory efficient - one object at a time.
     *
     * Uses the client's default mapper unless overridden.
     *
     * @template T of object
     *
     * @param class-string<T> $class Target class for mapping
     * @param null|RowMapper $mapper Override default mapper for this iteration
     *
     * @return \Generator<int, T>
     */
    public function map(string $class, ?RowMapper $mapper = null) : \Generator;

    /**
     * Fetch the next row. Returns null when exhausted.
     *
     * @return null|array<string, mixed>
     */
    public function next() : ?array;
}
