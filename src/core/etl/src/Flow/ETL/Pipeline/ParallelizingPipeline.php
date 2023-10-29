<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @internal
 */
final class ParallelizingPipeline implements Pipeline
{
    private readonly Pipeline $nextPipeline;

    /**
     * @param int<1, max> $parallel
     */
    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly int $parallel
    ) {
        $this->nextPipeline = $pipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline, $this->parallel);
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $this->pipeline->closure($rows, $context);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return $this->pipeline->isAsync();
    }

    public function process(FlowContext $context) : \Generator
    {
        $this->nextPipeline->source(
            From::chunks_from(
                From::pipeline($this->pipeline),
                $this->parallel
            )
        );

        return $this->nextPipeline->process($context);
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
