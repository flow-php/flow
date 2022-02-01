<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @internal
 */
interface Pipeline
{
    /**
     * Create clean instance of pipeline, without any transformers/loaders registered.
     */
    public function clean() : self;

    public function registerTransformer(Transformer $transformer) : void;

    public function registerLoader(Loader $loader) : void;

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     * @param callable(Rows $rows) : void $callback
     */
    public function process(\Generator $generator, callable $callback = null) : void;

    public function onError(ErrorHandler $errorHandler) : void;
}
