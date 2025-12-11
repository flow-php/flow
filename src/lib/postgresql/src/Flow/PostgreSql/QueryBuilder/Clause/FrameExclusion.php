<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

/**
 * Window frame exclusion enum.
 */
enum FrameExclusion : string
{
    case CURRENT_ROW = 'CURRENT ROW';
    case GROUP = 'GROUP';
    case NO_OTHERS = 'NO OTHERS';
    case TIES = 'TIES';
}
