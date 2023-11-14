<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Pipeline;

/**
 * Pipelines implementing OverridingPipeline interface overrides one or more pipelines.
 * This interface is required by Execution Plan / Optimizer to fully understand execution plan.
 *
 * Examples: NestedPipeline
 */
interface OverridingPipeline
{
    /**
     * @return array<Pipeline>
     */
    public function pipelines() : array;
}
