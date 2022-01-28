<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class CollectingPipeline implements Pipeline
{
    private Pipeline $pipeline;

    private Pipeline $nextPipeline;

    public function __construct(Pipeline $pipeline)
    {
        $this->pipeline = $pipeline;
        $this->nextPipeline = $pipeline->clean();
    }

    public function clean() : Pipeline
    {
        return new self($this->pipeline);
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
        $rows = (new Rows())->merge(...\iterator_to_array($this->pipeline->process($generator)));
        $rows = $rows->makeFirst()->makeLast();

        foreach ($this->nextPipeline->process($this->generate($rows)) as $nextRows) {
            yield $nextRows;
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
