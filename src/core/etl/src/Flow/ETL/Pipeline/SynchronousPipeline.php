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

    public function has(string $transformerClass) : bool
    {
        return $this->pipes->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return false;
    }

    /**
     * @psalm-suppress PossiblyNullOperand
     */
    public function process(FlowContext $context) : \Generator
    {
        $plan = $context
            ->config
            ->processors()
            ->process(new Pipeline\Execution\LogicalPlan($this->extractor, $this->pipes), $context);

        $generator = $plan->extractor->extract($context);
        $limiter = new Limiter($context->config->limit());

        while ($generator->valid()) {
            $rows = $limiter->limit($generator->current());
            $generator->next();

            if ($rows === null) {
                foreach ($plan->pipes->all() as $pipe) {
                    if ($pipe instanceof Pipeline\Closure) {
                        $pipe->closure($limiter->latest(), $context);
                    }
                }

                break;
            }

            foreach ($plan->pipes->all() as $pipe) {
                try {
                    if ($pipe instanceof Transformer) {
                        $rows = $limiter->limitTransformed($pipe->transform($rows, $context));
                    } elseif ($pipe instanceof Loader) {
                        $pipe->load($rows, $context);
                    }

                    if ($pipe instanceof Pipeline\Closure) {
                        if ($generator->valid() === false) {
                            $pipe->closure($rows, $context);
                        }
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
    }

    public function source(Extractor $extractor) : self
    {
        $this->extractor = $extractor;

        return $this;
    }
}
