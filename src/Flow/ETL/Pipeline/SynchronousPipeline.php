<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\Extractor;
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

    /**
     * @psalm-suppress PossiblyNullOperand
     */
    public function process(Config $config) : \Generator
    {
        $total = 0;

        $generator = $this->extractor->extract();

        while ($generator->valid()) {
            /** @var Rows $rows */
            $rows = $generator->current();
            $total += $rows->count();
            $generator->next();

            if ($config->hasLimit()) {
                if ($total > $config->limit()) {
                    $rows = $rows->dropRight($total - $config->limit());
                    $total = $config->limit();
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
                        } elseif ($config->limit() !== null && $total === $config->limit()) {
                            $pipe->closure($rows);
                        }
                    }
                } catch (\Throwable $exception) {
                    if ($config->errorHandler()->throw($exception, $rows)) {
                        throw $exception;
                    }

                    if ($config->errorHandler()->skipRows($exception, $rows)) {
                        break;
                    }
                }
            }

            yield $rows;

            if ($config->hasLimit() && $total === $config->limit()) {
                break;
            }
        }
    }

    public function source(Extractor $extractor) : self
    {
        $this->extractor = $extractor;

        return $this;
    }
}
