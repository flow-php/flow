<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Window\FrameAccumulator;

/**
 * Opts a WindowFunction into incremental frame evaluation. Carrying it asserts the result depends only
 * on the frame contents - never on the current row or its index - which is what lets WindowProcessor
 * reuse a value across rows that share frame bounds.
 */
interface FrameAccumulating
{
    public function accumulator(FlowContext $context): FrameAccumulator;
}
