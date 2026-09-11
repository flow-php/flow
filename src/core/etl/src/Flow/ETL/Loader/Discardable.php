<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\FlowContext;

/**
 * Loaders implementing this adapter will be notified by the pipeline when a run ends without reaching its last set of
 * Rows - it threw, or the caller walked away from the generator.
 */
interface Discardable
{
    public function discard(FlowContext $context): void;
}
