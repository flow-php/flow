<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\CacheExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use Throwable;
use WeakMap;

final readonly class Executor
{
    /**
     * A context advanced twice at once is a plan reading back from itself.
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
     * Plans and runs a whole plan inside one DataFrame span of $context: a planning failure closes the span as failed.
     *
     * @param null|RunCounts $counts counts the rows every source yields, a joined frame's included, and the peak of
     *                                every launched worker
     *
     * @throws InvalidLogicException when $context is already being advanced
     *
     * @return Generator<int, Rows, mixed, PhysicalPlan> returns the physical plan it ran
     */
    public function execute(LogicalPlan $logical, FlowContext $context, ?RunCounts $counts = null): Generator
    {
        if ($this->advancing->offsetExists($context)) {
            throw InvalidLogicException::cyclicPlanOnRun();
        }

        $this->advancing[$context] = true;
        $context->telemetry()->dataFrameStarted($context->config->telemetryAttributes());
        $plan = null;

        try {
            $plan = $context->config->planner()->plan($logical);

            foreach ($this->executePlan($plan, $context, $counts) as $rows) {
                // disarmed across our own yield: while parked we are not advancing, so a second read
                // arriving here is another reader of the same plan, not recursion
                $this->advancing->offsetUnset($context);

                yield $rows;

                $this->advancing[$context] = true;
            }
        } catch (Throwable $e) {
            $context->telemetry()->dataFrameFailed($e);

            throw $e;
        } finally {
            $this->advancing->offsetUnset($context);
            // drained, abandoned, or destroyed because the consumer's body threw - PHP runs this finally in
            // every case, and it is a no-op once dataFrameFailed() closed the span
            $context->telemetry()->dataFrameCompleted();
            $plan?->refreshSources();
        }

        return $plan;
    }

    /**
     * Executes $logical and merges every batch into one Rows.
     *
     * @throws InvalidLogicException when $context is already being advanced
     */
    public function fetch(LogicalPlan $logical, FlowContext $context): Rows
    {
        return $this->merge($this->execute($logical, $context), $context->backend());
    }

    /**
     * Merges every batch into one Rows. An empty result still carries the schema of the plan the generator returns,
     * or an empty one when that plan cannot describe its rows.
     *
     * @param Generator<int, Rows, mixed, PhysicalPlan> $batches what execute() yields
     */
    public function merge(Generator $batches, Backend $backend): Rows
    {
        $last = null;
        $parts = [];

        foreach ($batches as $batch) {
            $last = $batch;

            if (!$batch->isEmpty()) {
                $parts[] = $batch;
            }
        }

        if ($parts !== []) {
            return Rows::concat($parts[0]->schema(), $backend, ...$parts);
        }

        if ($last !== null) {
            return $last;
        }

        try {
            return Rows::empty($batches->getReturn()->schema(), $backend);
        } catch (SchemaNotDerivableException) {
            return Rows::empty(new Schema(), $backend);
        }
    }

    /**
     * Drives a plan with no DataFrame span - $context belongs to whoever runs it. The config's launcher may launch the
     * head of the plan's chain; without one, or when the launch does not pay, the plan runs here.
     *
     * @param null|RunCounts $counts counts the rows the plan's source yields and the peak of every launched worker
     *
     * @return Generator<int, Rows>
     */
    public function executePlan(PhysicalPlan $plan, FlowContext $context, ?RunCounts $counts = null): Generator
    {
        return $this->run($plan->root(), $context, $plan, $counts, null, new StepRunner());
    }

    /**
     * Drives one pipeline and the pipelines it reads, all in this process, with no DataFrame span.
     *
     * @param null|Extractor $source feeds the chain's first pipeline when it reads no source of its own
     * @param StepRunner $runner a branch's own, so the steps its writes become stay the same across its fibers
     *
     * @throws InvalidLogicException when the chain reads no source and none is given
     *
     * @return Generator<int, Rows>
     */
    public function executePipeline(
        Pipeline $pipeline,
        FlowContext $context,
        ?Extractor $source = null,
        StepRunner $runner = new StepRunner(),
    ): Generator {
        return $this->run($pipeline, $context, null, null, $source, $runner);
    }

    /**
     * @param null|PhysicalPlan $plan the plan $pipeline roots, which the config's launcher may launch; null runs here
     *
     * @return Generator<int, Rows>
     */
    private function run(
        Pipeline $pipeline,
        FlowContext $context,
        ?PhysicalPlan $plan,
        ?RunCounts $counts,
        ?Extractor $fed,
        StepRunner $runner,
    ): Generator {
        $chain = $pipeline->chain();
        $read = $chain[0]->input;
        $extractor = $read instanceof Read ? $read->extractor() : null;
        $source = match (true) {
            $extractor instanceof CacheExtractor => $extractor->boundTo($context->cache()),
            $extractor !== null => $extractor,
            default => $fed ?? throw InvalidLogicException::because('A pipeline that reads no source must be fed'),
        };
        $criteria = $read instanceof Read ? $read->criteria() : new Criteria();
        $counted = $read instanceof Read ? $counts : null;
        $launch =
            $plan !== null && $source instanceof SplittableExtractor
                ? $context->config->launcher()?->read($plan, $source, $criteria, $context, $counted)
                : null;
        // the launch ran the pipelines before the last one it consumed, and that one up to the launch's rest
        $last = $launch === null ? -1 : $launch->consumed - 1;
        $generator = $launch->rows ?? (new AdoptedBatches($context->backend()))->of($source->extract(
            $context->backend(),
            $criteria,
        ));

        // the launcher counts its own source
        if ($counted !== null && $launch === null) {
            $generator = $counted->count($source, $generator, $criteria->narrows());
        }

        foreach ($chain as $position => $stage) {
            if ($position < $last) {
                continue;
            }

            $generator =
                $position === $last && $launch !== null
                    ? $runner->steps($launch->rest, $generator, $context, counts: $counts)
                    : $runner->steps($stage->steps, $generator, $context, $position === 0 ? $source : null, $counts);
        }

        // a foreach, never `yield from`: the re-yield must not forward the consumer's sent signal; every stage routes
        // or refuses a corrupted record, so one left here had no stage to meet
        foreach ($generator as $rows) {
            yield $rows instanceof Rows ? $rows : throw $rows;
        }
    }
}
