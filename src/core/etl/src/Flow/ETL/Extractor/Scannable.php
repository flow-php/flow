<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

/**
 * A source that takes the planner's pushed parameters at read time. Scan::$limit is a hint - the Limit step
 * above the source enforces the exact count, so a source that reads more, or ignores it, is still correct.
 */
interface Scannable extends Extractor
{
    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, Scan $scan = new Scan()): Generator;
}
