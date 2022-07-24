<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

/**
 * Loaders implementing this adapter will be additionally notified
 * by the pipeline about the last set of Rows processed by the pipeline.
 */
interface Closure
{
    public function closure(Rows $rows, FlowContext $context) : void;
}
