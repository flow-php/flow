<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Pipeline\BoundPlan;
use Flow\ETL\Pipeline\PlanBinder;
use Flow\ETL\Pipeline\Segments;
use Generator;

/**
 * @internal
 */
final class Pipeline
{
    private ?SchemaNotDerivableException $bindRefusal = null;

    private bool $binding = false;

    private ?BoundPlan $bound = null;

    private bool $running = false;

    private readonly Segments $segments;

    public function __construct(
        private readonly Extractor $extractor,
    ) {
        $this->segments = new Segments();
    }

    public function add(Transformer|Loader|Processor $step): self
    {
        $this->invalidateBind();
        $this->segments->add($step);

        return $this;
    }

    /**
     * Walk the plan once and memoise the result, refusal included.
     *
     * @throws InvalidLogicException
     * @throws SchemaNotDerivableException
     */
    public function bind(): BoundPlan
    {
        if ($this->bindRefusal !== null) {
            throw $this->bindRefusal;
        }

        if ($this->bound !== null) {
            return $this->bound;
        }

        if ($this->binding) {
            throw InvalidLogicException::cyclicPlanOnDescribe();
        }

        $this->binding = true;

        try {
            return $this->bound = (new PlanBinder())->bind($this->extractor, $this->segments);
        } catch (SchemaNotDerivableException $refusal) {
            $this->bindRefusal = $refusal;

            throw $refusal;
        } finally {
            $this->binding = false;
        }
    }

    public function boundOrNull(): ?BoundPlan
    {
        try {
            return $this->bind();
        } catch (SchemaNotDerivableException) {
            return null;
        }
    }

    /**
     * Get the pipeline extractor.
     */
    public function extractor(): Extractor
    {
        return $this->extractor;
    }

    /**
     * Check if pipeline contains a step of the given class.
     *
     * @param class-string<Loader|Processor|Transformer> $class
     */
    public function has(string $class): bool
    {
        return $this->segments->has($class);
    }

    /**
     * Drop the memo. Call after any mutation of the plan or of its extractor - both change what the
     * walk would compute.
     */
    public function invalidateBind(): void
    {
        $this->bound = null;
        $this->bindRefusal = null;
    }

    /**
     * Process the pipeline and yield Rows batches.
     *
     * @return \Generator<int, Rows>
     */
    public function process(FlowContext $context): Generator
    {
        if ($this->running) {
            throw InvalidLogicException::cyclicPlanOnRun();
        }

        $this->running = true;

        try {
            $generator = $this->extractor->extract($context);

            foreach (($this->boundOrNull()?->segments() ?? $this->segments)->all() as $segment) {
                $generator = $segment->execute($generator, $context);

                $processor = $segment->processor();

                if ($processor !== null) {
                    $generator = $processor->process($generator, $context);
                }
            }

            // disarmed across our own yield: while parked we are not advancing, so a second read
            // arriving here is another reader of the same plan, not recursion. Only a re-entry during
            // the advance is a cycle - every step is pulled from inside this foreach.
            foreach ($generator as $rows) {
                $this->running = false;

                yield $rows;

                $this->running = true;
            }
        } finally {
            $this->running = false;
        }
    }

    /**
     * Get the pipeline stages.
     */
    public function segments(): Segments
    {
        return $this->segments;
    }
}
