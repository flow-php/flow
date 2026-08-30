<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\LoaderTree;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Generator;
use SplObjectStorage;
use Throwable;

use function array_map;
use function array_merge;
use function count;

/**
 * A segment of the pipeline containing Transformers/Loaders until a Processor boundary.
 *
 * @internal
 */
final readonly class Segment
{
    /** @var \SplObjectStorage<Loader|Transformer, null> */
    private SplObjectStorage $steps;

    public function __construct(
        private ?Processor $processor = null,
    ) {
        /** @var \SplObjectStorage<Loader|Transformer, null> $steps */
        $steps = new SplObjectStorage();
        $this->steps = $steps;
    }

    public function add(Transformer|Loader $step): void
    {
        $this->steps->offsetSet($step);
    }

    public function contains(Transformer|Loader|Processor $step): bool
    {
        if ($step instanceof Processor) {
            return $this->processor === $step;
        }

        return $this->steps->offsetExists($step);
    }

    /**
     * Execute this segment's Transformers and Loaders on the input generator.
     *
     * @param \Generator<Rows> $input
     *
     * @return \Generator<Rows>
     */
    public function execute(Generator $input, FlowContext $context): Generator
    {
        $loaders = [];

        foreach ($this->steps as $step) {
            if ($step instanceof Loader) {
                $loaders[] = $step;
            }
        }

        $completed = false;
        $endings = [];

        try {
            while ($input->valid()) {
                $rows = $input->current();
                $input->next();

                if ($rows === null) {
                    continue;
                }

                foreach ($this->steps as $step) {
                    try {
                        if ($step instanceof Transformer) {
                            try {
                                $rows = $step->transform($rows, $context);
                            } catch (LimitReachedException $e) {
                                $context->telemetry()->limitReached(['limit' => $e->limit]);
                                $rows = new Rows($rows->schema());
                                $input->send(Signal::STOP);
                            }
                        } elseif ($rows->count()) {
                            $step->load($rows, $context);
                        }
                    } catch (Throwable $exception) {
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

                if (count($rows)) {
                    yield $rows;
                }
            }

            $completed = true;
        } finally {
            $endings = $this->endLoaders($loaders, $context, $completed);
        }

        // unreachable while an exception is in flight, so a failed run keeps its own exception
        if ($endings !== []) {
            throw $endings[0];
        }
    }

    /**
     * A completed run ends only the outermost loader: a wrapper's closure() drains its stream before forwarding, and
     * that ordering is the wrapper's to own. A dead run has no such ordering, and a wrapper that forgets to forward
     * would strand the sink it wraps - so discarding walks the whole loader tree instead of trusting each wrapper.
     *
     * @param array<Loader> $loaders
     *
     * @return array<Throwable> failures raised while ending, empty when the run did not complete
     */
    private function endLoaders(array $loaders, FlowContext $context, bool $completed): array
    {
        $ending = [];

        if (!$completed) {
            $tree = new LoaderTree();
            $loaders = array_merge(...array_map(static fn(Loader $loader): array => $tree->flatten($loader), $loaders));
        }

        foreach ($loaders as $loader) {
            try {
                if ($completed) {
                    if ($loader instanceof Closure) {
                        $loader->closure($context);
                    }
                } elseif ($loader instanceof Discardable) {
                    $loader->discard($context);
                }
            } catch (Throwable $failure) {
                // one sink failing to end must not strand the others
                if ($completed) {
                    $ending[] = $failure;
                } else {
                    $context
                        ->telemetry()
                        ->logger()
                        ->error('Loader failed to end after a failed run.', [
                            'exception' => $failure,
                        ]);
                }
            }
        }

        return $ending;
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
            $segment->steps->offsetSet($step);
        }

        return $segment;
    }
}
