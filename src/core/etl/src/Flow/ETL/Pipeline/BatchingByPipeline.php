<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\{batched_by, from_pipeline};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Rows, Transformer};
use Flow\ETL\Row\Reference;

final readonly class BatchingByPipeline implements OverridingPipeline, Pipeline
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Pipeline $pipeline,
        private Reference $column,
        private ?int $minSize = null,
    ) {
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
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

    public function pipelines() : array
    {
        return [$this->pipeline];
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    /**
     * @return \Generator<int, Rows>
     */
    public function process(FlowContext $context) : \Generator
    {
        return batched_by(from_pipeline($this->pipeline), $this->column, $this->minSize)->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
