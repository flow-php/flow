<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Exception\SinkFailure;
use Flow\ETL\Exception\TransactionRolledBack;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Generator;
use SplObjectStorage;
use Throwable;

use function count;

/**
 * A segment of the pipeline containing Transformers/Loaders until a Processor boundary.
 */
final readonly class Segment
{
    /** @var \SplObjectStorage<Loader|Transformer, null> */
    private SplObjectStorage $steps;

    /**
     * @param null|Extractor $extractor the source this segment reads directly; null behind a processor
     */
    public function __construct(
        private ?Processor $processor = null,
        private ?Extractor $extractor = null,
    ) {
        /** @var \SplObjectStorage<Loader|Transformer, null> $steps */
        $steps = new SplObjectStorage();
        $this->steps = $steps;
    }

    public function add(Transformer|Loader $step): void
    {
        $this->steps->offsetSet($step);
    }

    /**
     * Execute this segment's Transformers and Loaders on the input generator.
     *
     * @param Generator<Rows> $input
     *
     * @return Generator<int, Rows, Signal|null, void>
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
        $started = false;

        try {
            while (true) {
                try {
                    // resuming the input runs the source, so it has to happen inside the try that answers for it
                    if ($started) {
                        $input->next();
                    }

                    $started = true;

                    if (!$input->valid()) {
                        break;
                    }

                    $rows = $input->current();
                } catch (Throwable $extraction) {
                    // behind a processor the input is an upstream segment, which already put its failures to the handler
                    if ($this->extractor === null) {
                        throw $extraction;
                    }

                    if (
                        $context->errorHandler()->onExtraction(new ExtractionError($extraction, $this->extractor))
                        === ExtractionAction::propagate
                    ) {
                        $context
                            ->telemetry()
                            ->logger()
                            ->error('Error during extraction.', ['exception' => $extraction]);

                        throw $extraction;
                    }

                    // a generator that threw cannot be resumed, so skipping the failed batch ends the source
                    break;
                }

                if ($rows === null) {
                    continue;
                }

                // the batch is half transformed, so its columns no longer match its siblings -
                // skipping means emitting nothing, not emitting what the failed step had produced
                $skipped = false;
                $stop = false;

                foreach ($this->steps as $step) {
                    try {
                        if ($step instanceof Transformer) {
                            $rows = $step->transform($rows, $context);
                        } elseif ($rows->count()) {
                            $step->load($rows, $context);
                        }
                    } catch (LimitReachedException $limit) {
                        $context->telemetry()->limitReached(['limit' => $limit->limit]);
                        // the remaining steps still run, on the trimmed batch
                        $rows = $limit->rows ?? new Rows($rows->schema());
                        $stop = true;
                    } catch (Throwable $failure) {
                        if ($failure instanceof SinkFailure) {
                            throw $failure->cause;
                        }

                        if ($failure instanceof TransactionRolledBack) {
                            $step = $failure->loader;
                            $failure = $failure->cause;
                        }

                        if ($step instanceof Transformer) {
                            if (
                                $context->errorHandler()->onTransformation(new TransformationError(
                                    $failure,
                                    $step,
                                    $rows,
                                )) === TransformationAction::propagate
                            ) {
                                $context
                                    ->telemetry()
                                    ->logger()
                                    ->error('Error during ETL segment execution.', ['exception' => $failure]);

                                throw $failure;
                            }

                            $context
                                ->telemetry()
                                ->logger()
                                ->debug('Skipping rows due to error during ETL segment execution.', [
                                    'exception' => $failure,
                                ]);

                            $skipped = true;

                            break;
                        }

                        if (
                            $context->errorHandler()->onLoading(new LoadingError($failure, $step, $rows))
                            === LoadingAction::propagate
                        ) {
                            $context
                                ->telemetry()
                                ->logger()
                                ->error('Error during ETL segment execution.', ['exception' => $failure]);

                            throw $failure;
                        }

                        $context
                            ->telemetry()
                            ->logger()
                            ->debug('Skipping loader due to error during ETL segment execution.', [
                                'exception' => $failure,
                            ]);
                    }
                }

                if (!$skipped && count($rows)) {
                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        $stop = true;
                    }
                }

                if ($stop) {
                    $input->send(Signal::STOP);

                    // a stop is a completed run - the loaders still close
                    break;
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
     * Every planner-built wrapper forwards discard() to its children (SinkFeed, TransactionalSinks), so each step is
     * ended directly - there is no user-supplied wrapper left to distrust.
     *
     * @param array<Loader> $loaders
     *
     * @return array<Throwable> failures raised while ending, empty when the run did not complete
     */
    private function endLoaders(array $loaders, FlowContext $context, bool $completed): array
    {
        $ending = [];

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

                    // a closure() that threw published at most part of its output; the rest is abandoned as on
                    // a failed run
                    if ($loader instanceof Discardable) {
                        try {
                            $loader->discard($context);
                        } catch (Throwable $discardFailure) {
                            $context
                                ->telemetry()
                                ->logger()
                                ->error('Loader failed to discard after its closure failed.', [
                                    'exception' => $discardFailure,
                                ]);
                        }
                    }
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

    public function extractor(): ?Extractor
    {
        return $this->extractor;
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
        $segment = new self($processor, $this->extractor);

        foreach ($this->steps as $step) {
            $segment->steps->offsetSet($step);
        }

        return $segment;
    }
}
