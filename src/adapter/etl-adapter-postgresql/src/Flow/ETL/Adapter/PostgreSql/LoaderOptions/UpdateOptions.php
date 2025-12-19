<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\LoaderOptions;

/**
 * Configuration options for UPDATE operations in PostgreSQL loader.
 */
final readonly class UpdateOptions
{
    /**
     * @param list<string> $primaryKeys Columns to use in WHERE clause for matching rows
     */
    public function __construct(
        public array $primaryKeys,
    ) {
    }
}
