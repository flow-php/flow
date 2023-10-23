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

final class SynchronousPipeline implements Pipeline
{
    private Extractor $extractor;

    private readonly Pipes $pipes;

    public function __construct()
    {
        $this->pipes = Pipes::empty();
        $this->extractor = From::rows(new Rows());
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipes->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new self();
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        foreach ($this->pipes->all() as $pipe) {
            if ($pipe instanceof Pipeline\Closure) {
                $pipe->closure($rows, $context);
            }
        }
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipes->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return false;
    }

    public function process(FlowContext $context) : \Generator
    {
        $plan = $context
            ->config
            ->processors()
            ->process(new Pipeline\Execution\LogicalPlan($this->extractor, $this->pipes), $context);

        $generator = $plan->extractor->extract($context);

        $rows = new Rows();

        while ($generator->valid()) {
            $rows = $generator->current();
            $generator->next();

            foreach ($plan->pipes->all() as $pipe) {
                try {
                    if ($pipe instanceof Transformer) {
                        $rows = $pipe->transform($rows, $context);
                    } elseif ($pipe instanceof Loader) {
                        $pipe->load($rows, $context);
                    }
                } catch (\Throwable $exception) {
                    if ($context->errorHandler()->throw($exception, $rows)) {
                        throw $exception;
                    }

                    if ($context->errorHandler()->skipRows($exception, $rows)) {
                        break;
                    }
                }
            }

            yield $rows;
        }

        $this->closure($rows, $context);
    }

    public function source(Extractor $extractor) : self
    {
        $this->extractor = $extractor;

        return $this;
    }
}
