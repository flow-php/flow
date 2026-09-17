<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\Format;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Stage;

/**
 * A frame's plan, frozen: the logical plan and the context it runs under. Later verbs on the frame do not reach it.
 */
final readonly class Plan
{
    private function __construct(
        public LogicalPlan $logical,
        public FlowContext $context,
    ) {}

    /**
     * A copy sharing the TelemetryContext would leave the embedded frame's DataFrame span unbalanced when both
     * frames were built on one FlowContext.
     */
    public static function of(LogicalPlan $logical, FlowContext $context): self
    {
        return new self($logical, (new FlowContext($context->config))->setErrorHandler($context->errorHandler()));
    }

    /**
     * A subtree several consumers share is printed once and referenced by its number afterwards. The optimized stage
     * is the plan the configured optimizer hands to the planner; a joined or read frame is part of the tree.
     */
    public function toString(Stage $stage = Stage::optimized, Format $format = Format::tree): string
    {
        return (new Explain())->of(match ($stage) {
            Stage::unoptimized => $this->logical,
            Stage::optimized => $this->context->config->optimizer()->optimize($this->logical, $this->context),
        }, $format);
    }
}
