<?php declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\FlowContext;

/**
 * Loaders implementing this adapter will be additionally notified
 * by the pipeline about the last set of Rows processed by the pipeline.
 */
interface Closure
{
    public function closure(FlowContext $context) : void;
}
