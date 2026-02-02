<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{FlowContext, Loader, Processor, Rows, Transformer};
use Flow\ETL\Loader\Closure;

/**
 * A segment of the pipeline containing Transformers/Loaders until a Processor boundary.
 *
 * @internal
 */
final class Segment
{
    /** @var array<Loader|Transformer> */
    private array $steps = [];

    public function __construct(private readonly ?Processor $processor = null)
    {
    }

    public function add(Transformer|Loader $step) : void
    {
        $this->steps[] = $step;
    }

    /**
     * Execute this segment's Transformers and Loaders on the input generator.
     *
     * @param \Generator<Rows> $input
     *
     * @return \Generator<Rows>
     */
    public function execute(\Generator $input, FlowContext $context) : \Generator
    {
        $loaders = [];

        foreach ($this->steps as $step) {
            if ($step instanceof Loader) {
                $loaders[] = $step;
            }
        }

        while ($input->valid()) {
            $rows = $input->current();
            $input->next();

            foreach ($this->steps as $step) {
                try {
                    if ($step instanceof Transformer) {
                        try {
                            $rows = $step->transform($rows, $context);
                        } catch (LimitReachedException) {
                            $rows = new Rows();
                            $input->send(Signal::STOP);
                        }
                    } elseif ($step instanceof Loader && $rows->count()) {
                        $step->load($rows, $context);
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

        foreach ($loaders as $loader) {
            if ($loader instanceof Closure) {
                $loader->closure($context);
            }
        }
    }

    /**
     * Check if segment contains a step of the given class.
     *
     * @param class-string<Loader|Processor|Transformer> $class
     */
    public function has(string $class) : bool
    {
        if ($this->processor instanceof $class) {
            return true;
        }

        foreach ($this->steps as $step) {
            if ($step instanceof $class) {
                return true;
            }
        }

        return false;
    }

    public function processor() : ?Processor
    {
        return $this->processor;
    }

    /**
     * @return array<Loader|Transformer>
     */
    public function steps() : array
    {
        return $this->steps;
    }

    public function withProcessor(Processor $processor) : self
    {
        $segment = new self($processor);
        $segment->steps = $this->steps;

        return $segment;
    }
}
