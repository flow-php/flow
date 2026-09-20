<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Transformer;

/**
 * A transformer that keeps state between batches. Every run of a frame starts from its own fresh instance, so the
 * state never carries over from an earlier run.
 */
interface Stateful extends Transformer
{
    /**
     * A new instance in the state this one was constructed in.
     */
    public function fresh(): self;
}
