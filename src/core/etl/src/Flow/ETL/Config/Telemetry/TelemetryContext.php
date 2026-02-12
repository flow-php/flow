<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\ETL\{DataFrame,
    Dataset\Memory\Consumption,
    Dataset\Statistics\HighResolutionTime,
    FlowContext,
    Loader,
    Pipeline\Optimizer\Optimization,
    Rows,
    Transformer};
use Flow\Filesystem\Filesystem;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Meter\Instrument\{Counter, Throughput};
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Tracer\{Span, SpanKind, SpanStatus, Tracer};

/**
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final class TelemetryContext
{
    private ?Counter $counterProcessedRows = null;

    private ?HighResolutionTime $dataFrameExecutionTime = null;

    private ?Span $dataFrameSpan = null;

    private ?Span $loadingSpan = null;

    private Consumption $memory;

    private ?Throughput $throughputRows = null;

    private int $totalRowsProcessed = 0;

    private ?Span $transformationSpan = null;

    public function __construct(
        private readonly Logger $logger,
        private readonly Tracer $tracer,
        private readonly Meter $meter,
        public readonly TelemetryOptions $options,
    ) {
        $this->memory = new Consumption();
    }

    public function dataFrameBatchProcessed(Rows $rows, FlowContext $context) : void
    {
        $this->totalRowsProcessed += $rows->count();
        $this->memory->capture();

        if ($this->options->collectMetrics) {
            $this->counterProcessedRows?->add($rows->count());
            $this->throughputRows?->add($rows->count());
        }
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function dataFrameCompleted(FlowContext $context, array $attributes = []) : void
    {
        if ($this->dataFrameSpan === null) {
            return;
        }

        $this->logger()->debug(
            'Data frame processing completed',
            [
                'dataframe_id' => $context->config->id(),
                'total_rows_processed' => $this->totalRowsProcessed,
                'memory_min_mb' => $this->memory->min()->inMb(),
                'memory_max_mb' => $this->memory->max()->inMb(),
            ],
            spanContext: $this->dataFrameSpan->context()
        );

        $throughput = 0.0;

        if ($this->dataFrameExecutionTime !== null) {
            $durationSeconds = HighResolutionTime::now()->diff($this->dataFrameExecutionTime)->toSeconds();
            $throughput = $durationSeconds > 0 ? \round($this->totalRowsProcessed / $durationSeconds, 2) : 0.0;
        }

        $this->tracer->complete(
            $this->dataFrameSpan
                ->setAttributes(\array_merge(
                    $attributes,
                    [
                        'dataframe.id' => $context->config->id(),
                        'rows.total' => $this->totalRowsProcessed,
                        'rows.throughput.per_second' => $throughput,
                        'memory.min.mb' => $this->memory->min()->inMb(),
                        'memory.max.mb' => $this->memory->max()->inMb(),
                    ]
                ))
                ->setStatus(SpanStatus::ok())
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
        $this->totalRowsProcessed = 0;
        $this->dataFrameSpan = null;
        $this->memory = new Consumption();
    }

    public function dataFrameStarted(FlowContext $context) : void
    {
        $this->dataFrameSpan = $this->tracer->span(DataFrame::class);

        $this->logger()->debug(
            'Data frame processing started',
            [
                'dataframe_id' => $context->config->id(),
                'cache' => $context->cache()::class,
                'serializer' => $context->config->serializer()::class,
                'optimizers' => \array_map(static fn (Optimization $optimization) => $optimization::class, $context->config->optimizer()->optimizations()),
                'telemetry' => [
                    'trace_loading' => $this->options->traceLoading,
                    'trace_transformations' => $this->options->traceTransformations,
                    'collect_metrics' => $this->options->collectMetrics,
                ],
                'fstab' => \array_map(static fn (Filesystem $filesystem) => $filesystem::class, $context->config->fstab()->filesystems()),
            ],
            spanContext: $this->dataFrameSpan->context()
        );

        if ($this->options->collectMetrics) {
            $this->counterProcessedRows = $this->meter->createCounter('rows.processed.total', 'Rows Processed');
            $this->throughputRows = $this->meter->createThroughput('rows.processed.throughput', 'Rows Processed');
        }

        $this->dataFrameExecutionTime = HighResolutionTime::now();
        $this->totalRowsProcessed = 0;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingCompleted(Loader $loader, array $attributes = []) : void
    {
        if ($this->loadingSpan === null) {
            return;
        }
        $this->tracer->complete(
            $this->loadingSpan
                ->setAttributes($attributes)
                ->setStatus(SpanStatus::ok())
        );

        $this->loadingSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingFailed(Loader $loader, \Throwable $exception, array $attributes = []) : void
    {
        if ($this->loadingSpan === null) {
            return;
        }

        $this->logger->error('Loading failed', ['exception' => $exception->getMessage(), 'loader' => $loader::class]);
        $this->tracer->complete(
            $this->loadingSpan
                ->setAttributes($attributes)
                ->setStatus(SpanStatus::error($exception->getMessage()))
        );

        $this->loadingSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingStarted(Loader $loader, array $attributes = []) : void
    {
        if ($this->options->traceLoading === false) {
            return;
        }

        $this->loadingSpan = $this->tracer->span(
            $loader::class,
            SpanKind::INTERNAL,
            Attributes::create($attributes),
            parentContext: $this->dataFrameSpan?->context()
        );
    }

    public function logger() : Logger
    {
        return $this->logger;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationCompleted(Transformer $transformer, array $attributes = []) : void
    {
        if ($this->transformationSpan === null) {
            return;
        }

        $this->tracer->complete(
            $this->transformationSpan
                ->setAttributes($attributes)
                ->setStatus(SpanStatus::ok())
        );
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationFailed(Transformer $transformer, \Throwable $exception, array $attributes = []) : void
    {
        if ($this->transformationSpan === null) {
            return;
        }

        $this->logger->error('Transformation failed', ['exception' => $exception->getMessage(), 'transformer' => $transformer::class]);
        $this->tracer->complete(
            $this->transformationSpan
                ->setAttributes($attributes)
                ->setStatus(SpanStatus::error($exception->getMessage()))
        );

        $this->transformationSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationStarted(Transformer $transformer, array $attributes = []) : void
    {
        if (!$this->options->traceTransformations) {
            return;
        }

        $this->transformationSpan = $this->tracer->span(
            $transformer::class,
            SpanKind::INTERNAL,
            Attributes::create($attributes),
            parentContext: $this->dataFrameSpan?->context()
        );
    }
}
