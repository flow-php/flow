<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class ParallelizingPipeline implements Pipeline
{
    private Pipeline $pipeline;

    private Pipeline $nextPipeline;

    private int $parallel;

    public function __construct(Pipeline $pipeline, int $parallel)
    {
        if ($parallel < 1) {
            throw new InvalidArgumentException("Parallel value can't be lower than 1.");
        }

        $this->pipeline = $pipeline;
        $this->parallel = $parallel;
        $this->nextPipeline = $pipeline->clean();
    }

    public function clean() : Pipeline
    {
        return new self($this->pipeline, $this->parallel);
    }

    public function registerTransformer(Transformer $transformer) : void
    {
        $this->nextPipeline->registerTransformer($transformer);
    }

    public function registerLoader(Loader $loader) : void
    {
        $this->nextPipeline->registerLoader($loader);
    }

    public function process(\Generator $generator) : \Generator
    {
        foreach ($this->pipeline->process($generator) as $rows) {
            foreach ($rows->chunks($this->parallel) as $chunk) {
                foreach ($this->nextPipeline->process($this->generate($chunk)) as $nextRows) {
                    yield $nextRows;
                }
            }
        }
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->nextPipeline->onError($errorHandler);
    }

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    private function generate(Rows $rows) : \Generator
    {
        yield $rows;
    }
}
