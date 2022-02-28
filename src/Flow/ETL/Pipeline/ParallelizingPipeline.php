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
    private Pipeline $nextPipeline;

    private int $parallel;

    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline, int $parallel)
    {
        if ($parallel < 1) {
            throw new InvalidArgumentException("Parallel value can't be lower than 1.");
        }

        $this->pipeline = $pipeline;
        $this->parallel = $parallel;
        $this->nextPipeline = $pipeline->clean();
    }

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function clean() : Pipeline
    {
        return new self($this->pipeline, $this->parallel);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(\Generator $generator, ?int $limit = null, callable $callback = null) : void
    {
        $this->pipeline->process($generator, $limit, function (Rows $rows) use ($limit, $callback) : void {
            foreach ($rows->chunks($this->parallel) as $chunk) {
                $this->nextPipeline->process($this->generate($chunk), $limit, $callback);
            }
        });
    }

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    private function generate(Rows $rows) : \Generator
    {
        yield $rows;
    }
}
