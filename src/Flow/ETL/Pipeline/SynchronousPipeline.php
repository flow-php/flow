<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @internal
 */
final class SynchronousPipeline implements Pipeline
{
    private ErrorHandler $errorHandler;

    private Pipes $pipes;

    public function __construct()
    {
        $this->errorHandler = new ThrowError();
        $this->pipes = Pipes::empty();
    }

    public function clean() : Pipeline
    {
        $newPipeline = new self();
        $newPipeline->errorHandler = $this->errorHandler;

        return $newPipeline;
    }

    public function add(Pipe $pipe) : void
    {
        $this->pipes->add($pipe);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->errorHandler = $errorHandler;
    }

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     */
    public function process(\Generator $generator, callable $callback = null) : void
    {
        $index = 0;

        while ($generator->valid()) {
            /** @var Rows $rows */
            $rows = $generator->current();
            $generator->next();

            if ($index === 0) {
                $rows = $rows->makeFirst();
            }

            if ($generator->valid() === false) {
                $rows = $rows->makeLast();
            }

            foreach ($this->pipes->all() as $element) {
                try {
                    if ($element instanceof Transformer) {
                        $rows = $element->transform($rows);
                    } elseif ($element instanceof Loader) {
                        $element->load($rows);
                    }
                } catch (\Throwable $exception) {
                    if ($this->errorHandler->throw($exception, $rows)) {
                        throw $exception;
                    }

                    if ($this->errorHandler->skipRows($exception, $rows)) {
                        break;
                    }
                }
            }

            if ($callback !== null) {
                $callback($rows);
            }

            $index++;
        }
    }
}
