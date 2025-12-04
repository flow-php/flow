<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{batches, from_pipeline};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Rows, Transformer};

final readonly class BatchingPipeline implements OverridingPipeline, Pipeline
{
    /**
     * @param Pipeline $pipeline
     * @param int<1, max> $size
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private Pipeline $pipeline, private int $size)
    {
        if ($this->size <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->size);
        }
    }

    #[\Override]
    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    #[\Override]
    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    #[\Override]
    public function pipelines() : array
    {
        return [$this->pipeline];
    }

    #[\Override]
    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    /**
     * @return \Generator<int, Rows>
     */
    #[\Override]
    public function process(FlowContext $context) : \Generator
    {
        return batches(from_pipeline($this->pipeline), $this->size)->extract($context);
    }

    #[\Override]
    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
