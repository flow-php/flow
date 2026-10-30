<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Fiber;
use Flow\ETL\IO\Extractor\FeedExtractor;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Runtime\FlowContext;
use Throwable;

/**
 * Owns the Fiber over one branch pipeline.
 */
final class BranchFiber
{
    private ?Fiber $fiber = null;

    /**
     * The LAST fiber ran to its end. Survives drop(), so a dropped terminated run stays terminated.
     */
    private bool $ended = false;

    /**
     * The last fiber ran to its end without throwing. A fiber the garbage collector destroyed while it was suspended is
     * terminated too, but never completed - its PipelineSteps already discarded the side loaders.
     */
    private bool $completed = false;

    public function __construct(
        private readonly Pipeline $side,
        private readonly Executor $executor,
        private readonly FlowContext $context,
        private readonly FeedExtractor $feed,
        private readonly StepRunner $runner,
    ) {}

    public function advance(): void
    {
        $this->fiber ??= new Fiber(function (): void {
            foreach ($this->executor->executePipeline($this->side, $this->context, $this->feed, $this->runner) as $_) {
            }
        });

        if ($this->fiber->isTerminated()) {
            return;
        }

        try {
            $this->fiber->isStarted() ? $this->fiber->resume() : $this->fiber->start();
        } catch (Throwable $failure) {
            $this->completed = false;

            throw $failure;
        }

        $this->completed = $this->fiber->isTerminated();
    }

    public function terminated(): bool
    {
        return $this->fiber?->isTerminated() ?? $this->ended;
    }

    public function completed(): bool
    {
        return $this->completed;
    }

    /**
     * Destroying a SUSPENDED fiber unwinds it into the side PipelineSteps' finally, now rather than at the next GC; after a
     * TERMINATED one the next advance() builds a fresh Fiber over the same pipeline, whose bound steps carry over.
     */
    public function drop(): void
    {
        $this->ended = $this->terminated();
        $this->fiber = null;
    }
}
