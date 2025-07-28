<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\{InvalidArgumentException};
use Flow\ETL\Extractor;
use Flow\ETL\{FlowContext, Loader, Pipeline, Transformer};

final readonly class OffsetPipeline implements OverridingPipeline, Pipeline
{
    /**
     * @param int<0, max> $offset
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private Pipeline $pipeline, private int $offset)
    {
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $this->offset);
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

    public function process(FlowContext $context) : \Generator
    {
        if ($this->offset === 0) {
            yield from $this->pipeline->process($context);

            return;
        }

        $skippedRows = 0;

        foreach ($this->pipeline->process($context) as $rows) {
            $currentBatchSize = $rows->count();
            $remainingToSkip = $this->offset - $skippedRows;

            if ($remainingToSkip >= $currentBatchSize) {
                $skippedRows += $currentBatchSize;

                continue;
            }

            if ($remainingToSkip > 0) {
                $rows = $rows->drop($remainingToSkip);
                $skippedRows += $remainingToSkip;
            }

            if ($rows->count() > 0) {
                yield $rows;
            }
        }
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
