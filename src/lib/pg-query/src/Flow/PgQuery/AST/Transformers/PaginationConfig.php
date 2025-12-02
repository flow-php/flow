<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

/**
 * Configuration for pagination transformations.
 */
final readonly class PaginationConfig
{
    public function __construct(
        public int $limit,
        public int $offset = 0,
        public ExistingLimitBehavior $existingLimitBehavior = ExistingLimitBehavior::COMBINE_MINIMUM,
    ) {
    }
}
