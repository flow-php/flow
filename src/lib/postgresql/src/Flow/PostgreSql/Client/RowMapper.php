<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

use Flow\PostgreSql\Client\RowMapper\Context;

/**
 * Contract for mapping database rows to typed results.
 *
 * @template-covariant T
 */
interface RowMapper
{
    /**
     * Map a database row to a typed result.
     *
     * @param array<string, mixed> $row Database row as associative array
     * @param Context $context Per-query context carrying the originating Query, executing Client, optional Catalog, and user-supplied data
     *
     * @throws Exception\MappingException When mapping fails
     *
     * @return T
     */
    public function map(array $row, Context $context): mixed;
}
