<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

/**
 * Opts a WindowFunction into incremental frame evaluation. Carrying it asserts the result depends only
 * on the frame contents - never on the current row or its index - which is what lets FrameAccumulation
 * reuse a value across rows that share frame bounds.
 */
interface FrameAccumulating
{
    /**
     * A fresh instance with no window, fed the frame's rows through aggregate().
     */
    public function accumulator(): AggregatingFunction;
}
