<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{chunks_from, from_pipeline};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

final readonly class BatchingPipeline implements Pipeline
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

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        return chunks_from(from_pipeline($this->pipeline), $this->size)->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
