<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Pipe;

/**
 * @internal
 */
interface Pipeline
{
    public function add(Pipe $pipe) : void;

    /**
     * Create clean instance of pipeline, with empty pipes.
     */
    public function clean() : self;

    public function onError(ErrorHandler $errorHandler) : void;

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     * @param callable(Rows $rows) : void $callback
     */
    public function process(\Generator $generator, ?int $limit = null, callable $callback = null) : void;
}
