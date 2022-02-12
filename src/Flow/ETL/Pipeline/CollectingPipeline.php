<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @internal
 */
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

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function process(\Generator $generator, callable $callback = null) : void
    {
        $rows = [];

        while ($generator->valid()) {
            $this->pipeline->process($generator, function (Rows $nextRows) use (&$rows) : void {
                /** @psalm-suppress MixedArrayAssignment */
                $rows[] = $nextRows;
            });
        }

        /** @var array<Rows> $rows */
        $mergedRows = (new Rows())->merge(...$rows)->makeFirst()->makeLast();

        $this->nextPipeline->process($this->generate($mergedRows), $callback);
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
