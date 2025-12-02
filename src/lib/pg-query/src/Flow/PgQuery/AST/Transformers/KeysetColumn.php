<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

/**
 * Defines a column for keyset pagination.
 */
final readonly class KeysetColumn
{
    public function __construct(
        public string $column,
        public SortOrder $order = SortOrder::ASC,
    ) {
    }
}
