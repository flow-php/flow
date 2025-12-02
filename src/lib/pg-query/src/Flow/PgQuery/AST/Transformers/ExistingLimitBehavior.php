<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

/**
 * Defines how pagination modifiers should handle existing LIMIT/OFFSET clauses.
 */
enum ExistingLimitBehavior
{
    /**
     * Use min(existing, new) for LIMIT and add OFFSET values.
     */
    case COMBINE_MINIMUM;

    /**
     * Throw PaginationException if LIMIT already exists.
     */
    case ERROR_IF_EXISTS;
    /**
     * Override existing LIMIT/OFFSET with new values.
     */
    case OVERRIDE;

    /**
     * Skip modification if LIMIT already exists.
     */
    case SKIP_IF_EXISTS;
}
