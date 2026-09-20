<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum Stage
{
    case unoptimized;

    case optimized;

    /**
     * The plan the executor runs, with every algorithm and its storage already chosen. Reaching it plans the frame,
     * so a source that infers its schema by reading is read here, which the logical stages never do.
     */
    case physical;
}
