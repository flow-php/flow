<?php

declare(strict_types=1);

namespace Flow\ETL\Execution;

use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Planner;
use Flow\ETL\Rows;
use Generator;
use Throwable;

/**
 * @internal one Run per DataFrame, per DataFrameExtractor and per FrameOutput: $running is per instance,
 *           and a side input is consumed while the outer Run is armed
 */
final class Run
{
    private bool $running = false;

    public function __construct(
        private readonly Planner $planner,
        private readonly Executor $executor,
    ) {}

    public static function in(Config $config): self
    {
        return new self($config->planner(), $config->executor());
    }

    /**
     * A planning failure is reported in a span of its own: a failure is never reported without a start.
     */
    public function plan(LogicalPlan $logical, FlowContext $context): Plan
    {
        try {
            return $this->planner->plan($logical, $context);
        } catch (Throwable $e) {
            $context->telemetry()->dataFrameStarted($context);
            $context->telemetry()->dataFrameFailed($context, $e);

            throw $e;
        }
    }

    /**
     * @return Generator<int, Rows>
     */
    public function of(LogicalPlan $logical, FlowContext $context): Generator
    {
        yield from $this->execute($this->plan($logical, $context));
    }

    /**
     * @throws InvalidLogicException when this Run is already advancing a plan
     *
     * @return Generator<int, Rows>
     */
    public function execute(Plan $plan): Generator
    {
        if ($this->running) {
            throw InvalidLogicException::cyclicPlanOnRun();
        }

        // the plan was built with this context (PipelineSplit hands it to the root Pipeline), so there is
        // no way to open a span on a context the plan does not belong to
        $context = $plan->root()->context();
        $this->running = true;
        $context->telemetry()->dataFrameStarted($context);

        try {
            foreach ($this->executor->execute($plan->root()) as $rows) {
                // disarmed across our own yield: while parked we are not advancing, so a second read
                // arriving here is another reader of the same plan, not recursion
                $this->running = false;

                yield $rows;

                $this->running = true;
            }
        } catch (Throwable $e) {
            $context->telemetry()->dataFrameFailed($context, $e);

            throw $e;
        } finally {
            $this->running = false;
            // drained, abandoned, or destroyed because the consumer's body threw - PHP runs this finally in
            // every case, and it is a no-op once dataFrameFailed() closed the span
            $context->telemetry()->dataFrameCompleted($context);
        }
    }
}
