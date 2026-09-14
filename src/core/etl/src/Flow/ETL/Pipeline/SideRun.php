<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Fiber;
use Flow\ETL\Executor;
use Flow\ETL\Plan;
use Throwable;

/**
 * @internal owns the Fiber over one side pipeline
 */
final class SideRun
{
    private ?Fiber $fiber = null;

    /**
     * The LAST fiber ran to its end. Survives drop(), so a dropped terminated run stays terminated.
     */
    private bool $ended = false;

    /**
     * The last fiber ran to its end without throwing. A fiber the garbage collector destroyed while it was suspended is
     * terminated too, but never completed - its Segment already discarded the side loaders.
     */
    private bool $completed = false;

    public function __construct(
        private readonly Plan\Pipeline $side,
        private readonly Executor $executor = new Executor(),
    ) {}

    public function advance(): void
    {
        $this->fiber ??= new Fiber(function (): void {
            foreach ($this->executor->execute($this->side) as $_) {
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
     * Destroying a SUSPENDED fiber unwinds it into the side Segment's finally, now rather than at the next GC; after a
     * TERMINATED one the next advance() builds a fresh Fiber over the same pipeline, whose bound steps carry over.
     */
    public function drop(): void
    {
        $this->ended = $this->terminated();
        $this->fiber = null;
    }
}
