<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

/**
 * Asserts the result depends only on frame contents, never on the current row or index.
 */
interface FrameAccumulating
{
    /**
     * A fresh instance with no window, fed the frame's rows through aggregate().
     */
    public function accumulator(): AggregatingFunction;
}
