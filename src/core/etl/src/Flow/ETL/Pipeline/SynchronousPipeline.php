<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\from_rows;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Loader\Closure;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Rows, Transformer};

final readonly class SynchronousPipeline implements Pipeline
{
    private Extractor $extractor;

    private Pipes $pipes;

    public function __construct(?Extractor $extractor = null)
    {
        $this->pipes = Pipes::empty();
        $this->extractor = $extractor ?? from_rows(new Rows());
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipes->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipes->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->pipes;
    }

    public function process(FlowContext $context) : \Generator
    {
        $generator = $this->extractor->extract($context);

        while ($generator->valid()) {
            $rows = $generator->current();
            $generator->next();

            foreach ($this->pipes()->all() as $pipe) {
                try {
                    if ($pipe instanceof Transformer) {
                        try {
                            $rows = $pipe->transform($rows, $context);
                        } catch (LimitReachedException) {
                            $rows = new Rows();
                            $generator->send(Signal::STOP);
                        }
                    } elseif ($pipe instanceof Loader) {
                        if ($rows->count()) {
                            /**
                             * When there are no rows to load, we should not call the loader. This way we can avoid
                             * checking rows count inside the loader implementation.
                             */
                            $pipe->load($rows, $context);
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

            if (\count($rows)) {
                yield $rows;
            }
        }

        foreach ($this->pipes->all() as $pipe) {
            if ($pipe instanceof Loader && $pipe instanceof Closure) {
                $pipe->closure($context);
            }
        }
    }

    public function source() : Extractor
    {
        return $this->extractor;
    }
}
