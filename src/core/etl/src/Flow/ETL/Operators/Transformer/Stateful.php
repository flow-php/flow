<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

/**
 * A step that keeps state between batches. Every run of a frame starts from its own fresh instance, so the state never
 * carries over from an earlier run. A parallel read never ships it to a worker: one instance sees every batch.
 */
interface Stateful
{
    /**
     * A new instance in the state this one was constructed in.
     */
    public function fresh(): static;
}
