<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder;

interface Sql
{
    /**
     * Convert this query to a SQL string.
     */
    public function toSql(): string;
}
