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
    private Pipeline $nextPipeline;

    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline)
    {
        $this->pipeline = $pipeline;
        $this->nextPipeline = $pipeline->clean();
    }

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function clean() : Pipeline
    {
        return new self($this->pipeline);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(\Generator $generator, ?int $limit = null, callable $callback = null) : void
    {
        $rows = [];

        while ($generator->valid()) {
            $this->pipeline->process($generator, $limit, function (Rows $nextRows) use (&$rows) : void {
                /** @psalm-suppress MixedArrayAssignment */
                $rows[] = $nextRows;
            });
        }

        /** @var array<Rows> $rows */
        $mergedRows = (new Rows())->merge(...$rows);

        $this->nextPipeline->process($this->generate($mergedRows), $limit, $callback);
    }

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    private function generate(Rows $rows) : \Generator
    {
        yield $rows;
    }
}
