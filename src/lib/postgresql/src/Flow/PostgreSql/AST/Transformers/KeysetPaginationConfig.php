<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;

use function is_array;
use function sprintf;

/**
 * Configuration for keyset (cursor-based) pagination.
 *
 * @type CursorValue = string|int|float|bool
 */
final readonly class KeysetPaginationConfig
{
    /**
     * @param int|Parameter $limit Maximum number of rows to return, or the parameter that carries it
     * @param list<KeysetColumn> $columns Columns to use for keyset comparison (must match ORDER BY)
     * @param null|list<null|CursorValue>|Parameter $cursor Values from the last row of previous page, or the parameter
     *                                                      the first of them is bound to (null for first page); a
     *                                                      NULL value is rejected
     *
     * @throws PaginationException
     */
    public function __construct(
        public int|Parameter $limit,
        public array $columns,
        public array|Parameter|null $cursor = null,
    ) {
        if (is_array($cursor)) {
            foreach ($cursor as $index => $value) {
                if ($value === null) {
                    throw new PaginationException(sprintf(
                        'Keyset cursor value #%d is NULL; key columns must be non-null',
                        $index + 1,
                    ));
                }
            }
        }
    }
}
