<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

/**
 * Every run starts from a fresh instance; a parallel read never ships it to a worker.
 */
interface Stateful
{
    /**
     * A new instance in the state this one was constructed in.
     */
    public function fresh(): static;
}
