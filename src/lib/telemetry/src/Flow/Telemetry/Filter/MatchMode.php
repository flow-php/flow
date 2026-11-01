<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

/**
 * Pattern modes compare the string form; the rest apply the PHP operator with no coercion.
 */
enum MatchMode
{
    case EQUAL;
    case NOT_EQUAL;
    case GREATER_THAN;
    case GREATER_THAN_EQUAL;
    case LESS_THAN;
    case LESS_THAN_EQUAL;
    case REGEXP;
    case STARTS_WITH;
    case ENDS_WITH;
    case CONTAINS;
}
