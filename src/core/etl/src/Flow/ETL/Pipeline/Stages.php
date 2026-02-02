<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Loader, Processor, Transformer};

/**
 * Manages pipeline segments, grouping steps at Processor boundaries.
 *
 * @internal
 */
final class Stages
{
    private Segment $currentSegment;

    /** @var array<Segment> */
    private array $segments = [];

    public function __construct()
    {
        $this->currentSegment = new Segment();
    }

    public function add(Transformer|Loader|Processor $step) : void
    {
        if ($step instanceof Processor) {
            $this->segments[] = $this->currentSegment->withProcessor($step);
            $this->currentSegment = new Segment();
        } else {
            $this->currentSegment->add($step);
        }
    }

    /**
     * Get all segments including the current one.
     *
     * @return array<Segment>
     */
    public function all() : array
    {
        return [...$this->segments, $this->currentSegment];
    }

    /**
     * Get the current (most recent) segment.
     *
     * Returns the last completed segment if any exist, otherwise the current segment being built.
     */
    public function current() : Segment
    {
        if ($this->segments === []) {
            return $this->currentSegment;
        }

        return $this->segments[\count($this->segments) - 1];
    }

    /**
     * Check if any segment contains a step of the given class.
     *
     * @param class-string<Loader|Processor|Transformer> $class
     */
    public function has(string $class) : bool
    {
        foreach ($this->segments as $segment) {
            if ($segment->has($class)) {
                return true;
            }
        }

        return $this->currentSegment->has($class);
    }

    /**
     * Get all steps (Transformers, Loaders, Processors) flattened.
     *
     * @return array<Loader|Processor|Transformer>
     */
    public function steps() : array
    {
        $steps = [];

        foreach ($this->segments as $segment) {
            foreach ($segment->steps() as $step) {
                $steps[] = $step;
            }

            if ($segment->processor() !== null) {
                $steps[] = $segment->processor();
            }
        }

        foreach ($this->currentSegment->steps() as $step) {
            $steps[] = $step;
        }

        return $steps;
    }
}
