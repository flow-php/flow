<?php

declare(strict_types=1);

namespace Flow\ETL;

interface Sink
{
    /**
     * @param DataFrame $prefix a frame over the caller's chain, handed to this call only - its writes become the
     *                         caller's sinks, its other verbs never reach the caller
     */
    public function write(DataFrame $prefix): void;
}
