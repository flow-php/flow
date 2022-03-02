<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Extractor;
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

    private Extractor $extractor;

    private Pipes $pipes;

    public function __construct()
    {
        $this->errorHandler = new ThrowError();
        $this->pipes = Pipes::empty();
        $this->extractor = From::rows(new Rows());
    }

    public function add(Pipe $pipe) : void
    {
        $this->pipes->add($pipe);
    }

    public function clean() : Pipeline
    {
        $newPipeline = new self();
        $newPipeline->errorHandler = $this->errorHandler;

        return $newPipeline;
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->errorHandler = $errorHandler;
    }

    public function process(?int $limit = null, callable $callback = null) : \Generator
    {
        $total = 0;

        $generator = $this->extractor->extract();

        while ($generator->valid()) {
            /** @var Rows $rows */
            $rows = $generator->current();
            $total += $rows->count();
            $generator->next();

            if ($limit !== null) {
                if ($total > $limit) {
                    $rows = $rows->dropRight($total - $limit);
                    $total = $limit;
                }
            }

            foreach ($this->pipes->all() as $pipe) {
                try {
                    if ($pipe instanceof Transformer) {
                        $rows = $pipe->transform($rows);
                    } elseif ($pipe instanceof Loader) {
                        $pipe->load($rows);
                    }

                    if ($pipe instanceof Pipeline\Closure) {
                        if ($generator->valid() === false) {
                            $pipe->closure($rows);
                        } elseif ($limit !== null && $total === $limit) {
                            $pipe->closure($rows);
                        }
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

            yield $rows;

            if ($limit !== null && $total === $limit) {
                break;
            }
        }
    }

    public function source(Extractor $extractor) : void
    {
        $this->extractor = $extractor;
    }
}
