<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\CorruptedRecordsRouter;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileLoader;
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
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\ListDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Generator;
use Throwable;

use function count;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function sprintf;

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
     * @param Generator<CorruptedRecordException|Rows> $input
     * @param null|Extractor|Reader $source the source $input comes from directly, so its failures reach the error
     *                                      handler and its corrupted records its reject loader
     * @param null|SourceCounts $sources counts the rows the sources of a join's right plan yield
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function steps(
        PipelineSteps $steps,
        Generator $input,
        FlowContext $context,
        Extractor|Reader|null $source = null,
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
     * $input through one run of loaders and transformers: drops empty batches, ends its loaders when the run ends. A
     * corrupted record goes to the source's reject loader, loaded and ended like the run's own loaders; without one it
     * fails the run, whatever the error handler.
     *
     * @param list<Loader|NestedLoader|Transformer> $steps one run between two processors
     * @param Generator<CorruptedRecordException|Rows> $input
     * @param null|Extractor|Reader $source the source $input comes from directly, so its failures reach the error
     *                                      handler and its corrupted records its reject loader
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function run(
        array $steps,
        Generator $input,
        FlowContext $context,
        Extractor|Reader|null $source = null,
    ): Generator {
        $loaders = [];
        $limits = [];
        $rejects = $source instanceof CorruptedRecordsRouter ? $source->corruptedRecordsLoader() : null;
        /** @var list<array<string, mixed>> $corrupted */
        $corrupted = [];
        $routed = 0;
        if ($rejects !== null) {
            $loaders[] = $rejects;
        }

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

                if ($rows instanceof CorruptedRecordException) {
                    // the reader's option alone owns a corrupted record: no error handler skips it
                    if ($rejects === null) {
                        throw $rows;
                    }

                    $corrupted[] = $rows->toRecord();
                    $routed++;

                    continue;
                }

                // a batch's corrupted records come before its rows, so they are loaded together when the rows arrive
                if ($rejects !== null && $corrupted !== []) {
                    $this->loadRejects($rejects, $corrupted, $context);
                    $corrupted = [];
                }

                // the batch is half transformed, so its columns no longer match its siblings -
                // skipping means emitting nothing, not emitting what the failed step had produced
                $skipped = false;
                $stop = false;

                foreach ($steps as $step) {
                    if (!$step instanceof Transformer) {
                        $this->load($step, $rows, $context);

                        continue;
                    }

                    try {
                        $rows = $this->telemetry->transform($step, $rows, $context);
                    } catch (Throwable $failure) {
                        if (
                            $context->errorHandler()->onTransformation(new TransformationError($failure, $step, $rows))
                            === TransformationAction::propagate
                        ) {
                            $context
                                ->telemetry()
                                ->logger()
                                ->error('Error during ETL pipeline execution.', ['exception' => $failure]);

                            throw $failure;
                        }

                        $context
                            ->telemetry()
                            ->logger()
                            ->debug('Skipping rows due to error during ETL pipeline execution.', [
                                'exception' => $failure,
                            ]);

                        $skipped = true;

                        break;
                    }
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

            if ($rejects !== null && $corrupted !== []) {
                $this->loadRejects($rejects, $corrupted, $context);
            }

            $completed = true;
        } finally {
            $endings = $this->endLoaders($loaders, $context, $completed);
        }

        // unreachable while an exception is in flight, so a failed run keeps its own exception
        if ($endings !== []) {
            throw $endings[0];
        }

        if ($rejects !== null && $routed > 0) {
            $context
                ->telemetry()
                ->logger()
                ->warn(sprintf(
                    'Routed %d corrupted records to %s.',
                    $routed,
                    $rejects instanceof FileLoader ? $rejects->destination()->uri() : $rejects::class,
                ));
        }
    }

    /**
     * @param non-empty-list<array<string, mixed>> $records CorruptedRecordException::toRecord() of each
     */
    private function loadRejects(Loader $rejects, array $records, FlowContext $context): void
    {
        $this->load(
            $rejects,
            (new RowsBuilder(
                new Schema(
                    new StringDefinition('uri', nullable: true),
                    new IntegerDefinition('offset'),
                    new IntegerDefinition('line', nullable: true),
                    new StringDefinition('reason'),
                    new StringDefinition('record'),
                    new ListDefinition('fields', type_list(type_string()), nullable: true),
                ),
                $context->backend(),
            ))
                ->appendRecords($records)
                ->finish(),
            $context,
        );
    }

    /**
     * $rows into $loader; a failure the error handler skips is logged and the batch flows on, a rolled-back
     * transaction included.
     */
    private function load(Loader|NestedLoader $loader, Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        if ($loader instanceof Loader) {
            try {
                $this->telemetry->load($loader, $rows, $context);

                return;
            } catch (Throwable $failure) {
                $error = new LoadingError($failure, $loader, $rows);
            }
        } else {
            // what a NestedLoader throws is never offered here, see NestedLoader::load()
            $error = $loader->load($rows);
        }

        if ($error === null) {
            return;
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

        $context
            ->telemetry()
            ->logger()
            ->debug('Skipping loader due to error during ETL pipeline execution.', [
                'exception' => $error->cause,
            ]);
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
