<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipe;

/**
 * @internal
 */
interface Pipeline
{
    /**
     * Create clean instance of pipeline, with empty pipes.
     */
    public function clean() : self;

    public function add(Pipe $pipe) : void;

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     * @param callable(Rows $rows) : void $callback
     */
    public function process(\Generator $generator, callable $callback = null) : void;

    public function onError(ErrorHandler $errorHandler) : void;
}
