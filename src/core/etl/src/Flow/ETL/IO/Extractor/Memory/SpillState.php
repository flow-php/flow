<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\Memory;

/**
 * Not a bool pair: "spilled K of N, then the source threw" must never re-stream the source.
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
