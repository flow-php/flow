<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

/**
 * Configuration for keyset (cursor-based) pagination.
 *
 * @type CursorValue = string|int|float|bool|null
 */
final readonly class KeysetPaginationConfig
{
    /**
     * @param int $limit Maximum number of rows to return
     * @param list<KeysetColumn> $columns Columns to use for keyset comparison (must match ORDER BY)
     * @param null|list<CursorValue> $cursor Values from the last row of previous page (null for first page)
     */
    public function __construct(
        public int $limit,
        public array $columns,
        public ?array $cursor = null,
    ) {}
}
