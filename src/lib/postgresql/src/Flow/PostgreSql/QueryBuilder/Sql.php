<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder;

/**
 * Interface for objects that can be converted to SQL.
 *
 * Implemented by all query builder final step interfaces.
 * Allows the Client to accept both raw SQL strings and query builder objects.
 */
interface Sql
{
    /**
     * Convert this query to a SQL string.
     */
    public function toSql(): string;
}
