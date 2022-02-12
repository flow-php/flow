<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @internal
 */
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

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function process(\Generator $generator, callable $callback = null) : void
    {
        $this->pipeline->process($generator, function (Rows $rows) use ($callback) : void {
            foreach ($rows->chunks($this->parallel) as $chunk) {
                $this->nextPipeline->process($this->generate($chunk), $callback);
            }
        });
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
