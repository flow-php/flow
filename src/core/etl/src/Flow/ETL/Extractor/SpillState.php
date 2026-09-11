<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

/**
 * The lifecycle of one SpilledRows. Split into states rather than carried as booleans because the
 * reachable-but-unnamed fourth state - "spilled K of N rows, then the source threw" - is where a bool
 * pair silently re-streams a half-consumed source and loses rows.
 */
enum SpillState
{
    case Abandoned;
    case Deleted;
    case Fresh;
    case Replaying;
    case Spilled;
    case Spilling;
}
