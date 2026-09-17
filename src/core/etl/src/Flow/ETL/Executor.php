<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Extractor\FileExtractor;
use Generator;
use Throwable;
use WeakMap;

use function array_reverse;
use function in_array;
use function sprintf;

final readonly class Executor
{
    /**
     * The contexts of the plans this executor is advancing right now. Every DataFrame has its own context and every
     * embedded plan runs on its own copy, so a context advanced twice at once is a plan reading back from itself.
     *
     * @var WeakMap<FlowContext, true>
     */
    private WeakMap $advancing;

    public function __construct()
    {
        /** @var WeakMap<FlowContext, true> $advancing */
        $advancing = new WeakMap();
        $this->advancing = $advancing;
    }

    /**
     * Runs a whole plan inside one DataFrame span of the plan's context.
     *
     * @throws InvalidLogicException when the plan's context is already being advanced
     *
     * @return Generator<int, Rows>
     */
    public function execute(PhysicalPlan $plan): Generator
    {
        // the plan was built with this context (PipelineSplit hands it to the root Pipeline), so there is
        // no way to open a span on a context the plan does not belong to
        $context = $plan->root()->context();

        if ($this->advancing->offsetExists($context)) {
            throw InvalidLogicException::cyclicPlanOnRun();
        }

        $this->advancing[$context] = true;
        $context->telemetry()->dataFrameStarted($context);

        try {
            foreach ($this->executePipeline($plan->root()) as $rows) {
                // disarmed across our own yield: while parked we are not advancing, so a second read
                // arriving here is another reader of the same plan, not recursion
                $this->advancing->offsetUnset($context);

                yield $rows;

                $this->advancing[$context] = true;
            }
        } catch (Throwable $e) {
            $context->telemetry()->dataFrameFailed($context, $e);

            throw $e;
        } finally {
            $this->advancing->offsetUnset($context);
            // drained, abandoned, or destroyed because the consumer's body threw - PHP runs this finally in
            // every case, and it is a no-op once dataFrameFailed() closed the span
            $context->telemetry()->dataFrameCompleted($context);
        }
    }

    /**
     * Executes $plan and merges every batch into one Rows. An empty result still carries the plan's schema, or an
     * empty one when the plan cannot describe its rows.
     *
     * @throws InvalidLogicException when the plan's context is already being advanced
     */
    public function fetch(PhysicalPlan $plan): Rows
    {
        $rows = null;

        foreach ($this->execute($plan) as $nextRows) {
            $rows = $rows === null ? $nextRows : $rows->merge($nextRows);
        }

        if ($rows !== null) {
            return $rows;
        }

        try {
            return new Rows($plan->schema());
        } catch (SchemaNotDerivableException) {
            return new Rows(new Schema());
        }
    }

    /**
     * Drives one pipeline and the pipelines it reads. Only the frames inlined into it get a DataFrame span here -
     * the pipeline's own context belongs to whoever runs it.
     *
     * @return Generator<int, Rows>
     */
    public function executePipeline(Pipeline $pipeline): Generator
    {
        $chain = [];

        for ($stage = $pipeline; $stage !== null; $stage = $stage->input()) {
            $chain[] = $stage;
        }

        $chain = array_reverse($chain);
        $leaf = $chain[0];
        $source = $leaf->segments()->extractor() ?? throw InvalidLogicException::pipelineWithoutSource(sprintf(
            'pipeline #%d',
            $leaf->id,
        ));
        $generator = $source instanceof FileExtractor
            ? $source->extract($leaf->context(), $leaf->limit(), $leaf->pathFilter())
            : $source->extract($leaf->context(), $leaf->limit());

        $embedded = [];

        foreach ($chain as $stage) {
            $context = $stage->context();

            if ($context !== $pipeline->context() && !in_array($context, $embedded, true)) {
                $embedded[] = $context;
            }

            foreach ($stage->segments()->all() as $segment) {
                $generator = $segment->execute($generator, $context);
                $processor = $segment->processor();

                if ($processor !== null) {
                    $generator = $processor->process($generator, $context);
                }
            }
        }

        foreach ($embedded as $context) {
            $context->telemetry()->dataFrameStarted($context);
        }

        try {
            // a foreach, never `yield from`: the re-yield must not forward the consumer's sent signal
            foreach ($generator as $rows) {
                yield $rows;
            }
        } catch (Throwable $e) {
            foreach ($embedded as $context) {
                $context->telemetry()->dataFrameFailed($context, $e);
            }

            throw $e;
        } finally {
            // drained, abandoned, or destroyed because the consumer's body threw - the same shape as
            // execute(), and a no-op once dataFrameFailed() closed the span
            foreach ($embedded as $context) {
                $context->telemetry()->dataFrameCompleted($context);
            }
        }
    }
}
