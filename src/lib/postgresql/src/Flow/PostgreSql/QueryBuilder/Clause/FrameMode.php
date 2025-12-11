<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

/**
 * Window frame mode enum.
 */
enum FrameMode : string
{
    case GROUPS = 'GROUPS';
    case RANGE = 'RANGE';
    case ROWS = 'ROWS';
}
