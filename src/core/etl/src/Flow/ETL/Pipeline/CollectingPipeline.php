<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @internal
 */
final class CollectingPipeline implements Pipeline
{
    private readonly Pipeline $nextPipeline;

    /**
     * @param Pipeline $pipeline
     * @param null|int<1, max> $batchSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly Pipeline $pipeline, private readonly ?int $batchSize = null)
    {
        $this->nextPipeline = $pipeline->cleanCopy();

        if ($this->batchSize !== null) {
            /**
             * @psalm-suppress DocblockTypeContradiction
             *
             * @phpstan-ignore-next-line
             */
            if ($this->batchSize <= 0) {
                throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
            }
        }
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

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
        if ($this->batchSize === null) {
            $this->nextPipeline->source(From::rows(
                (new Rows())->merge(...\iterator_to_array($this->pipeline->process($context)))
            ));
        } else {
            $this->nextPipeline->source(
                From::chunks_from(
                    From::pipeline($this->pipeline),
                    $this->batchSize
                )
            );
        }

        return $this->nextPipeline->process($context);
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
