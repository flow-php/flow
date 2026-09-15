<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\QueryBuilder\Expression\Parameter;

/**
 * Configuration for keyset (cursor-based) pagination.
 *
 * @type CursorValue = string|int|float|bool|null
 */
final readonly class KeysetPaginationConfig
{
    /**
     * @param int|Parameter $limit Maximum number of rows to return, or the parameter that carries it
     * @param list<KeysetColumn> $columns Columns to use for keyset comparison (must match ORDER BY)
     * @param null|list<CursorValue>|Parameter $cursor Values from the last row of previous page, or the parameter the
     *                                                 first of them is bound to (null for first page)
     */
    public function __construct(
        public int|Parameter $limit,
        public array $columns,
        public array|Parameter|null $cursor = null,
    ) {}
}
