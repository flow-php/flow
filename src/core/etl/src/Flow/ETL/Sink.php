<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
interface Sink
{
    /**
     * @param DataFrame $prefix PRIVATE to this call - a fork over the frame's root with its own FlowContext copy
     *
     * @return Sinks
     */
    public function roots(DataFrame $prefix): array;
}
