<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class LimitingPipeline implements Pipeline
{
    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly int $limit
    ) {
        if ($this->limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: -1");
        }
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
    }

    public function closure(FlowContext $context) : void
    {
        $this->pipeline->closure($context);
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
        $total = 0;

        foreach ($this->pipeline->process($context) as $rows) {
            $total += $rows->count();

            if ($total === $this->limit) {
                yield $rows;
                $this->closure($context);

                return;
            }

            if ($total > $this->limit) {
                $diff = $total - $this->limit;

                if ($diff > 0) {
                    yield $rows->dropRight($diff);
                }

                $this->closure($context);

                return;
            }

            yield $rows;
        }
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
