<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Telemetry;

use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Consumption;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Meter\Instrument\Throughput;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\ObjectExtractor;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Throwable;

use function array_merge;
use function round;

/**
 * @import-type TAttributeValueMap from Attributes
 */
final class TelemetryContext
{
    private const string SPAN_NEVER_COMPLETED = 'Span was never completed.';

    private ?Counter $counterProcessedRows = null;

    private ?HighResolutionTime $dataFrameExecutionTime = null;

    private ?Scope $dataFrameScope = null;

    private ?Span $dataFrameSpan = null;

    private Consumption $memory;

    /**
     * Operator spans live across the batches they yield, so only the end of the data frame drains them.
     */
    private readonly SpanStack $operators;

    private readonly SpanStack $steps;

    private ?Throughput $throughputRows = null;

    private int $totalRowsProcessed = 0;

    public function __construct(
        private readonly string $dataFrameId,
        private readonly string $dataFrameName,
        private readonly Backend $backend,
        private readonly Logger $logger,
        private readonly Tracer $tracer,
        private readonly Meter $meter,
        public readonly TelemetryOptions $options,
    ) {
        $this->memory = new Consumption($this->backend);
        $this->operators = new SpanStack();
        $this->steps = new SpanStack();
    }

    public function dataFrameBatchProcessed(Rows $rows): void
    {
        $this->totalRowsProcessed += $rows->count();
        $this->memory->capture();

        $this->steps->drain($this->tracer, self::SPAN_NEVER_COMPLETED);

        if ($this->options->collectMetrics) {
            $attributes = [TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName];
            $this->counterProcessedRows?->add($rows->count(), $attributes);
            $this->throughputRows?->add($rows->count(), $attributes);
        }
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function dataFrameCompleted(array $attributes = []): void
    {
        $dataFrameSpan = $this->dataFrameSpan;

        if ($dataFrameSpan === null) {
            return;
        }

        $this->drain();
        $this->dataFrameScope?->detach();
        $this->dataFrameScope = null;

        $this->logger()->debug(
            'Data frame processing completed',
            [
                'dataframe_id' => $this->dataFrameId,
                'dataframe_name' => $this->dataFrameName,
                'total_rows_processed' => $this->totalRowsProcessed,
                'memory_min_mb' => $this->memory->min()->inMb(),
                'memory_max_mb' => $this->memory->max()->inMb(),
            ],
            spanContext: $dataFrameSpan->context(),
        );

        $throughput = 0.0;

        if ($this->dataFrameExecutionTime !== null) {
            $durationSeconds = HighResolutionTime::now()->diff($this->dataFrameExecutionTime)->toSeconds();
            $throughput = $durationSeconds > 0 ? round($this->totalRowsProcessed / $durationSeconds, 2) : 0.0;
        }

        $this->tracer->complete($dataFrameSpan->setAttributes(array_merge($attributes, [
            TelemetryAttributes::ATTR_DATAFRAME_ID => $this->dataFrameId,
            TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName,
            TelemetryAttributes::ATTR_ROWS_TOTAL => $this->totalRowsProcessed,
            TelemetryAttributes::ATTR_ROWS_THROUGHPUT => $throughput,
            TelemetryAttributes::ATTR_MEMORY_MIN => $this->memory->min()->inMb(),
            TelemetryAttributes::ATTR_MEMORY_MAX => $this->memory->max()->inMb(),
        ])));

        if ($this->counterProcessedRows !== null) {
            $this->meter->complete($this->counterProcessedRows);
            $this->counterProcessedRows = null;
        }

        if ($this->throughputRows !== null) {
            $this->meter->complete($this->throughputRows);
            $this->throughputRows = null;
        }

        $this->dataFrameExecutionTime = null;
        $this->dataFrameSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function dataFrameFailed(Throwable $exception, array $attributes = []): void
    {
        $dataFrameSpan = $this->dataFrameSpan;

        if ($dataFrameSpan === null) {
            return;
        }

        $this->drain();
        $this->dataFrameScope?->detach();
        $this->dataFrameScope = null;

        $this->logger->error('Data frame processing failed', [
            'exception' => $exception->getMessage(),
            'dataframe_id' => $this->dataFrameId,
            'dataframe_name' => $this->dataFrameName,
        ]);

        $throughput = 0.0;

        if ($this->dataFrameExecutionTime !== null) {
            $durationSeconds = HighResolutionTime::now()->diff($this->dataFrameExecutionTime)->toSeconds();
            $throughput = $durationSeconds > 0 ? round($this->totalRowsProcessed / $durationSeconds, 2) : 0.0;
        }

        $this->tracer->complete(
            $dataFrameSpan
                ->setAttributes(array_merge($attributes, [
                    TelemetryAttributes::ATTR_DATAFRAME_ID => $this->dataFrameId,
                    TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName,
                    TelemetryAttributes::ATTR_ROWS_TOTAL => $this->totalRowsProcessed,
                    TelemetryAttributes::ATTR_ROWS_THROUGHPUT => $throughput,
                    TelemetryAttributes::ATTR_MEMORY_MIN => $this->memory->min()->inMb(),
                    TelemetryAttributes::ATTR_MEMORY_MAX => $this->memory->max()->inMb(),
                ]))
                ->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class)
                ->setStatus(SpanStatus::error($exception->getMessage())),
        );

        if ($this->counterProcessedRows !== null) {
            $this->meter->complete($this->counterProcessedRows);
            $this->counterProcessedRows = null;
        }

        if ($this->throughputRows !== null) {
            $this->meter->complete($this->throughputRows);
            $this->throughputRows = null;
        }

        $this->dataFrameExecutionTime = null;
        $this->dataFrameSpan = null;
    }

    /**
     * @param TAttributeValueMap $configuration the run's settings, logged with the start
     */
    public function dataFrameStarted(array $configuration): void
    {
        $dataFrameSpan = $this->tracer->span(
            'DataFrame ' . $this->dataFrameName,
            SpanKind::INTERNAL,
            Attributes::create([
                TelemetryAttributes::ATTR_DATAFRAME_ID => $this->dataFrameId,
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName,
            ]),
        );
        $this->dataFrameSpan = $dataFrameSpan;
        // activated so that everything started while the data frame runs - including filesystem streams, which
        // never activate themselves - is parented to it
        $this->dataFrameScope = $this->tracer->activate($dataFrameSpan);

        $this->logger()->debug(
            'Data frame processing started',
            array_merge([
                'dataframe_id' => $this->dataFrameId,
                'dataframe_name' => $this->dataFrameName,
            ], $configuration),
            spanContext: $dataFrameSpan->context(),
        );

        if ($this->options->collectMetrics) {
            $this->counterProcessedRows = $this->meter->createCounter(
                'flow.etl.rows.processed',
                '{row}',
                'Total number of rows processed by the DataFrame',
            );
            $this->throughputRows = $this->meter->createThroughput(
                'flow.etl.rows.throughput',
                '{row}',
                'Rows processed per second',
            );
        }

        $this->dataFrameExecutionTime = HighResolutionTime::now();
        $this->totalRowsProcessed = 0;
        $this->memory = new Consumption($this->backend);
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function limitReached(array $attributes = []): void
    {
        $this->logger->debug('Limit reached, stopping the pipeline execution.', $attributes);
    }

    public function logger(): Logger
    {
        return $this->logger;
    }

    /**
     * The heap of the data frame started last, sampled once per batch; it stays readable after the data frame ends.
     */
    public function memory(): Consumption
    {
        return $this->memory;
    }

    /**
     * The rows that reached the end of the data frame started last; it stays readable after the data frame ends.
     */
    public function totalRows(): int
    {
        return $this->totalRowsProcessed;
    }

    /**
     * Completes the span stepStarted() opened; a span already completed or drained is left alone.
     *
     * @param TAttributeValueMap $attributes
     */
    public function stepCompleted(SpanScope $step, array $attributes = []): void
    {
        if (!$this->steps->remove($step) && !$this->operators->remove($step)) {
            return;
        }

        $step->scope?->detach();
        $this->tracer->complete($step->span->setAttributes($attributes));
    }

    /**
     * @param class-string $class
     * @param TAttributeValueMap $attributes
     */
    public function stepFailed(
        SpanScope $step,
        StepKind $kind,
        string $class,
        Throwable $exception,
        array $attributes = [],
    ): void {
        if (!($kind === StepKind::operator ? $this->operators : $this->steps)->remove($step)) {
            return;
        }

        $step->scope?->detach();
        $this->logger->error($kind->failure(), ['exception' => $exception->getMessage(), $kind->logKey() => $class]);
        $this->tracer->complete(
            $step
                ->span
                ->setAttributes($attributes)
                ->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class)
                ->setStatus(SpanStatus::error($exception->getMessage())),
        );
    }

    /**
     * Opens a step's span under the data frame span. An operator's span is not activated: it stays open across the
     * batches it yields, while other code runs.
     *
     * @param class-string $class
     * @param TAttributeValueMap $attributes
     */
    public function stepStarted(StepKind $kind, string $class, array $attributes = []): SpanScope
    {
        $span = $this->tracer->span(
            ObjectExtractor::shortName($class),
            SpanKind::INTERNAL,
            Attributes::create(array_merge([
                $kind->classAttribute() => $class,
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName,
            ], $attributes)),
            parent: $this->dataFrameParent(),
        );
        $step = new SpanScope($span, $kind === StepKind::operator ? null : $this->tracer->activate($span));
        ($kind === StepKind::operator ? $this->operators : $this->steps)->push($step);

        return $step;
    }

    /**
     * One span per task a launcher completed, named "$stage $task"; the spans of the steps shipped with it are lost.
     *
     * @param TAttributeValueMap $attributes
     */
    public function taskCompleted(int $task, string $stage, array $attributes = []): void
    {
        if (!$this->options->traceTransformations) {
            return;
        }

        $this->tracer->complete($this->tracer->span(
            $stage . ' ' . $task,
            SpanKind::INTERNAL,
            Attributes::create(array_merge([
                $stage => $task,
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataFrameName,
            ], $attributes)),
            parent: $this->dataFrameParent(),
        ));
    }

    /**
     * Derived from the current context so baggage and suppression are preserved.
     */
    private function dataFrameParent(): ?Context
    {
        $dataFrameSpan = $this->dataFrameSpan;

        if ($dataFrameSpan === null) {
            return null;
        }

        return $this->tracer->context()->withActiveSpan($dataFrameSpan->context());
    }

    private function drain(): void
    {
        $this->steps->drain($this->tracer, self::SPAN_NEVER_COMPLETED);
        $this->operators->drain($this->tracer, self::SPAN_NEVER_COMPLETED);
    }
}
