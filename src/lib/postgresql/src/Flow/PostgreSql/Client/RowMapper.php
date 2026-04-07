<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

/**
 * Contract for mapping database rows to typed results.
 *
 * @template T
 */
interface RowMapper
{
    /**
     * Map a database row to a typed result.
     *
     * @param array<string, mixed> $row Database row as associative array
     *
     * @throws Exception\MappingException When mapping fails
     *
     * @return T
     */
    public function map(array $row) : mixed;
}
