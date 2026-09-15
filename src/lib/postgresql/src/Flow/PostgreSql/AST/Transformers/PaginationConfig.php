<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\QueryBuilder\Expression\Parameter;

/**
 * Configuration for OFFSET-based pagination transformations.
 */
final readonly class PaginationConfig
{
    /**
     * @param int|Parameter $limit Maximum number of rows to return, or the parameter that carries it
     * @param int|Parameter $offset Number of rows to skip, or the parameter that carries it
     */
    public function __construct(
        public int|Parameter $limit,
        public int|Parameter $offset = 0,
    ) {}
}
