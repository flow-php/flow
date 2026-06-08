<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

/**
 * Comparison applied by an {@see AttributeRule} between the value found at its
 * path and the rule's expected value.
 *
 * The pattern modes ({@see self::REGEXP}, {@see self::STARTS_WITH},
 * {@see self::ENDS_WITH}, {@see self::CONTAINS}) operate on the string form of
 * the attribute value. The remaining modes apply the corresponding PHP operator
 * directly to the raw value - no type detection or coercion is performed.
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
