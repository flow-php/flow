<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * A segment of the pipeline containing Transformers/Loaders until a Processor boundary.
 *
 * @internal
 */
final readonly class Segment
{
    /** @var \SplObjectStorage<Loader|Transformer, null> */
    private \SplObjectStorage $steps;

    public function __construct(
        private ?Processor $processor = null,
    ) {
        $this->steps = new \SplObjectStorage();
    }

    public function add(Transformer|Loader $step): void
    {
        $this->steps->attach($step);
    }

    public function contains(Transformer|Loader|Processor $step): bool
    {
        if ($step instanceof Processor) {
            return $this->processor === $step;
        }

        return $this->steps->contains($step);
    }

    /**
     * Execute this segment's Transformers and Loaders on the input generator.
     *
     * @param \Generator<Rows> $input
     *
     * @return \Generator<Rows>
     */
    public function execute(\Generator $input, FlowContext $context): \Generator
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
                        } catch (LimitReachedException $e) {
                            $context
                                ->telemetry()
                                ->logger()
                                ->debug('Limit reached, stopping the pipeline execution.', ['limit_exception' => $e]);
                            $rows = new Rows();
                            $input->send(Signal::STOP);
                        }
                    } elseif ($rows->count()) {
                        $step->load($rows, $context);
                    }
                } catch (\Throwable $exception) {
                    if ($context->errorHandler()->throw($exception, $rows)) {
                        $context
                            ->telemetry()
                            ->logger()
                            ->error('Error during ETL segment execution.', ['exception' => $exception]);

                        throw $exception;
                    }

                    if ($context->errorHandler()->skipRows($exception, $rows)) {
                        $context
                            ->telemetry()
                            ->logger()
                            ->debug('Skipping rows due to error during ETL segment execution.', [
                                'exception' => $exception,
                            ]);

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
    public function has(string $class): bool
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

    public function processor(): ?Processor
    {
        return $this->processor;
    }

    /**
     * @return array<Loader|Transformer>
     */
    public function steps(): array
    {
        return iterator_to_array($this->steps);
    }

    public function withProcessor(Processor $processor): self
    {
        $segment = new self($processor);

        foreach ($this->steps as $step) {
            $segment->steps->attach($step);
        }

        return $segment;
    }
}
