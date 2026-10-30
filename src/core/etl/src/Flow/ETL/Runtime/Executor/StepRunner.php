<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\Limiting;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\LoadingAction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationAction;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;
use Generator;
use Throwable;

use function count;

final readonly class StepRunner
{
    public function __construct(
        private StepTelemetry $telemetry = new StepTelemetry(),
        private WriteLoaderFactory $writes = new WriteLoaderFactory(),
    ) {}

    /**
     * $input through every step and the breaker: every run of loaders and transformers between two processors or joins
     * goes to run(), an empty run included, and one more after the breaker. A join reads the rows of its right plan;
     * a branch runs as the step that feeds it.
     *
     * @param Generator<Rows> $input
     * @param null|Extractor $source the extractor $input comes from directly, so its failures reach the error handler
     * @param null|SourceCounts $sources counts the rows the sources of a join's right plan yield
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function steps(
        PipelineSteps $steps,
        Generator $input,
        FlowContext $context,
        ?Extractor $source = null,
        ?SourceCounts $sources = null,
    ): Generator {
        $run = [];

        foreach ($steps->breaker === null ? $steps->steps : [...$steps->steps, $steps->breaker] as $step) {
            $step = $this->writes->step($step, $context->errorHandler(), $context);

            if ($step instanceof Processor || $step instanceof JoinStep) {
                $upstream = $this->run($run, $input, $context, $source);
                $input = $step instanceof JoinStep
                    ? $this->telemetry->operator(
                        $step->join::class,
                        $step->join->join(
                            $upstream,
                            $context->config->executor()->executePlan($step->right, $context, $sources),
                            $context->backend(),
                        ),
                        $context,
                    )
                    : $this->telemetry->operator(
                        $step::class,
                        $step->process($upstream, $context->backend()),
                        $context,
                    );
                $source = null;
                $run = [];

                continue;
            }

            $run[] = $step;
        }

        return $this->run($run, $input, $context, $source);
    }

    /**
     * $input through one run of loaders and transformers: drops empty batches, ends its loaders when the run ends.
     *
     * @param list<Loader|NestedLoader|Transformer> $steps one run between two processors
     * @param Generator<Rows> $input
     * @param null|Extractor $source the extractor $input comes from directly, so its failures reach the error handler
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function run(array $steps, Generator $input, FlowContext $context, ?Extractor $source = null): Generator
    {
        $loaders = [];
        $limits = [];

        foreach ($steps as $step) {
            if (!$step instanceof Transformer) {
                $loaders[] = $step;
            } elseif ($step instanceof Limiting) {
                $limits[] = $step;
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
                    // behind a processor the input is an upstream run, which already put its failures to the
                    // handler
                    if ($source === null) {
                        throw $extraction;
                    }

                    if (
                        $context->errorHandler()->onExtraction(new ExtractionError($extraction, $source))
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

                foreach ($steps as $step) {
                    $error = null;

                    if ($step instanceof Transformer || $step instanceof Loader) {
                        try {
                            if ($step instanceof Transformer) {
                                $rows = $this->telemetry->transform($step, $rows, $context);
                            } elseif ($rows->count()) {
                                $this->telemetry->load($step, $rows, $context);
                            }
                        } catch (Throwable $failure) {
                            $error = $step instanceof Transformer
                                ? new TransformationError($failure, $step, $rows)
                                : new LoadingError($failure, $step, $rows);
                        }
                    } else {
                        // a NestedLoader: what it throws is never offered here, see NestedLoader::load()
                        $error = $rows->count() ? $step->load($rows) : null;
                    }

                    if ($error === null) {
                        continue;
                    }

                    if (
                        $error instanceof LoadingError
                            ? $context->errorHandler()->onLoading($error) === LoadingAction::propagate
                            : $context->errorHandler()->onTransformation($error) === TransformationAction::propagate
                    ) {
                        $context
                            ->telemetry()
                            ->logger()
                            ->error('Error during ETL pipeline execution.', ['exception' => $error->cause]);

                        throw $error->cause;
                    }

                    // a write that failed is skipped and the batch flows on, a rolled-back transaction included
                    if (!$step instanceof Transformer) {
                        $context
                            ->telemetry()
                            ->logger()
                            ->debug('Skipping loader due to error during ETL pipeline execution.', [
                                'exception' => $error->cause,
                            ]);

                        continue;
                    }

                    $context
                        ->telemetry()
                        ->logger()
                        ->debug('Skipping rows due to error during ETL pipeline execution.', [
                            'exception' => $error->cause,
                        ]);

                    $skipped = true;

                    break;
                }

                // a limit trims the batch it is reached in, so the steps after it still ran, on the trimmed batch
                foreach ($limits as $limit) {
                    if ($limit->reached()) {
                        $context->telemetry()->limitReached(['step' => $limit::class]);
                        $stop = true;
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
     * A NestedLoader forwards discard() to its writes, so each step is ended directly - there is no user-supplied
     * wrapper left to distrust.
     *
     * @param array<Loader|NestedLoader> $loaders
     *
     * @return array<Throwable> failures raised while ending, empty when the run did not complete
     */
    public function endLoaders(array $loaders, FlowContext $context, bool $completed): array
    {
        $ending = [];

        foreach ($loaders as $loader) {
            try {
                if ($completed) {
                    if ($loader instanceof Closure) {
                        $loader->closure();
                    }
                } elseif ($loader instanceof Discardable) {
                    $loader->discard();
                }
            } catch (Throwable $failure) {
                // one write failing to end must not strand the others
                if ($completed) {
                    $ending[] = $failure;

                    // a closure() that threw published at most part of its output; the rest is abandoned as on
                    // a failed run
                    if ($loader instanceof Discardable) {
                        try {
                            $loader->discard();
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
}
