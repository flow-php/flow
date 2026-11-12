<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\IO\CorruptedRecordsRouter;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
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
use ReflectionGenerator;
use ReflectionMethod;
use SplObjectStorage;
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
     * @param null|RunCounts $counts counts the rows the sources of a join's right plan yield
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function steps(
        PipelineSteps $steps,
        Generator $input,
        FlowContext $context,
        Extractor|Reader|null $source = null,
        ?RunCounts $counts = null,
    ): Generator {
        $run = [];

        foreach ($steps->breaker === null ? $steps->steps : [...$steps->steps, $steps->breaker] as $step) {
            $step = $this->writes->step($step, $context->errorHandler(), $context, $steps->stages, $steps->writes);

            if ($step instanceof Processor || $step instanceof JoinStep) {
                $input = $this->boundary(
                    $step,
                    $run,
                    $input,
                    $context,
                    $source,
                    $counts,
                    $steps->stages,
                    $steps->writes,
                );
                $source = null;
                $run = [];

                continue;
            }

            $run[] = $step;
        }

        return $this->run($run, $input, $context, $source, $steps->stages, $steps->writes);
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
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages the stage the planner built for each write and each join_each
     * @param null|SplObjectStorage<Loader, Progress> $writes the write stage the planner built for each loader
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function run(
        array $steps,
        Generator $input,
        FlowContext $context,
        Extractor|Reader|null $source = null,
        ?SplObjectStorage $stages = null,
        ?SplObjectStorage $writes = null,
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

        /** @var SplObjectStorage<Transformer, Progress> $joinStages */
        $joinStages = new SplObjectStorage();
        /** @var SplObjectStorage<Loader, Progress> $writeStages */
        $writeStages = new SplObjectStorage();

        foreach ($steps as $step) {
            if ($step instanceof Loader && $writes !== null && $writes->offsetExists($step)) {
                $writeStages->offsetSet($step, $writes->offsetGet($step));
            }

            if (!$step instanceof Transformer) {
                $loaders[] = $step;
            } elseif ($step instanceof Limiting) {
                $limits[] = $step;
            }

            if ($step instanceof Transformer && $stages !== null && $stages->offsetExists($step)) {
                $joinStages->offsetSet($step, $stages->offsetGet($step));
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

                // outside the extraction try, so no error handler turns a heartbeat failure into a source failure
                $context->config->heartbeat()?->beatWhenDue();

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
                        if ($step instanceof Loader && $writeStages->offsetExists($step) && $rows->count() > 0) {
                            $writeStages->offsetGet($step)->advanced($rows->count());
                        }

                        $this->load($step, $rows, $context);

                        continue;
                    }

                    if ($joinStages->offsetExists($step)) {
                        $joinStages->offsetGet($step)->advanced($rows->count());
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
        } catch (Throwable $failure) {
            if ($started) {
                $this->unwind($input, $failure, $context);
            }

            throw $failure;
        } finally {
            foreach ($joinStages as $step) {
                $joinStages->offsetGet($step)->done();
            }

            foreach ($writeStages as $step) {
                $writeStages->offsetGet($step)->done();
            }

            $endings = $this->endLoaders($loaders, $context, $completed, $stages);
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
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages the stage the planner built for each write and each join_each
     *
     * @return array<Throwable> failures raised while ending, empty when the run did not complete
     */
    public function endLoaders(
        array $loaders,
        FlowContext $context,
        bool $completed,
        ?SplObjectStorage $stages = null,
    ): array {
        $ending = [];
        $heartbeat = $context->config->heartbeat();

        foreach ($loaders as $loader) {
            // a stopped heartbeat ends the run, so nothing more is published
            $closing = $completed && !($heartbeat?->stopped() ?? false);
            // a write the planner did not see, such as a reject loader, gets its stage here
            $progress = $loader instanceof Loader && $stages !== null && $stages->offsetExists($loader)
                ? $stages->offsetGet($loader)
                : $context->config->progress()->stage(
                    StageName::commit,
                    ProgressUnit::files,
                    subject: $heartbeat !== null && $loader instanceof FileLoader
                        ? FileCommit::locationOf($loader->destination())
                        : null,
                );

            try {
                if ($closing) {
                    if ($loader instanceof Closure) {
                        $loader->closure($progress);
                    }
                } elseif ($loader instanceof Discardable) {
                    $loader->discard();
                }
            } catch (Throwable $failure) {
                // one write failing to end must not strand the others
                if ($closing) {
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

            $progress->done();
        }

        return $ending;
    }

    /**
     * @template T
     * @template TSend
     *
     * @param Generator<int, T, TSend, mixed> $batches
     *
     * @return Generator<int, T, TSend, void>
     */
    public function started(Generator $batches, bool &$started): Generator
    {
        // a failure thrown into a generator never started would start it: open its files, launch its workers
        $started = true;

        yield from $batches;
    }

    /**
     * Ends $input as failed: it must be the root of its `yield from` chain, as a throw() into a delegate is lost.
     */
    public function unwind(Generator $input, Throwable $failure, FlowContext $context): void
    {
        $rooted = false;

        try {
            // a generator closing under throw()'s frame can crash PHP; a yield-from delegate closes under a safe one
            ($input->valid() ? $this->started($input, $rooted) : $input)->throw($failure);
        } catch (Throwable $endFailure) {
            if ($endFailure !== $failure) {
                $context
                    ->telemetry()
                    ->logger()
                    ->warn('The input of a failed run failed to end.', ['exception' => $endFailure]);
            }
        }

        if ($input->valid()) {
            $function = (new ReflectionGenerator(
                (new ReflectionGenerator($input))->getExecutingGenerator(),
            ))->getFunction();
            $context
                ->telemetry()
                ->logger()
                ->warn(sprintf(
                    '%s kept running after the failure: rethrow it or return',
                    ($function instanceof ReflectionMethod ? $function->class . '::' : '') . $function->name . '()',
                ));
        }
    }

    /**
     * @param list<Loader|NestedLoader|Transformer> $run
     * @param Generator<CorruptedRecordException|Rows> $input
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages
     * @param null|SplObjectStorage<Loader, Progress> $writes
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    private function boundary(
        Processor|JoinStep $step,
        array $run,
        Generator $input,
        FlowContext $context,
        Extractor|Reader|null $source,
        ?RunCounts $counts,
        ?SplObjectStorage $stages,
        ?SplObjectStorage $writes,
    ): Generator {
        $upstreamStarted = false;
        $upstream = $this->started($this->run($run, $input, $context, $source, $stages, $writes), $upstreamStarted);

        if (!$step instanceof JoinStep) {
            $operator = $step->process($upstream, $context->backend());

            return $this->unwinding(
                $this->telemetry->operator($step::class, $operator, $context),
                $operator,
                $context,
                $upstream,
                $upstreamStarted,
            );
        }

        $rightStarted = false;
        $right = $this->started(
            $context->config->executor()->executePlan($step->right, $context, $counts),
            $rightStarted,
        );

        $operator = $step->join->join($upstream, $right, $context->backend());

        return $this->unwinding(
            $this->telemetry->operator($step->join::class, $operator, $context),
            $operator,
            $context,
            $upstream,
            $upstreamStarted,
            $right,
            $rightStarted,
        );
    }

    /**
     * @param Generator<Rows> $output what a processor or join yields, through its telemetry when traced
     * @param Generator<Rows> $operator the processor or join itself, $output when untraced
     * @param Generator<int, Rows, null|Signal, void> $input what it reads
     * @param null|Generator<int, Rows, mixed, void> $right what a join reads as its right side
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    private function unwinding(
        Generator $output,
        Generator $operator,
        FlowContext $context,
        Generator $input,
        bool &$inputStarted,
        ?Generator $right = null,
        bool &$rightStarted = false,
    ): Generator {
        try {
            // foreach resumes the operator from this frame: a generator closing under an internal call can crash PHP
            foreach ($output as $batch) {
                $signal = yield $batch;

                if ($signal === Signal::STOP) {
                    $rooted = false;
                    // a stop through a yield-from root resumes the operator under a user frame as well
                    $root = $this->started($operator, $rooted);
                    $root->current();
                    $root->send(Signal::STOP);

                    return;
                }
            }
        } catch (Throwable $failure) {
            $this->unwind($output, $failure, $context);
            // the telemetry around a traced operator never forwards the failure, so the operator gets it here
            $this->unwind($operator, $failure, $context);

            // an operator that failed on its own, or that reads with foreach, leaves its input suspended
            if ($inputStarted) {
                $this->unwind($input, $failure, $context);
            }

            if ($right !== null && $rightStarted) {
                $this->unwind($right, $failure, $context);
            }

            throw $failure;
        }
    }
}
