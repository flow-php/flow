<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

/**
 * Frame bound type enum for window frame specifications.
 */
enum FrameBoundType : string
{
    case CURRENT_ROW = 'CURRENT ROW';
    case FOLLOWING = 'FOLLOWING';
    case PRECEDING = 'PRECEDING';
    case UNBOUNDED_FOLLOWING = 'UNBOUNDED FOLLOWING';
    case UNBOUNDED_PRECEDING = 'UNBOUNDED PRECEDING';
}
