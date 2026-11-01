<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

enum Stage
{
    case unoptimized;

    case optimized;

    /**
     * Reaching it plans the frame, so a source that infers its schema is read here.
     */
    case physical;
}
