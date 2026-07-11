<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Optimizer\Optimization;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\Filesystem\Filesystem;
use Flow\Telemetry\Attributes;
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

use function array_map;
use function array_merge;
use function round;

/**
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final class TelemetryContext
{
    private ?FlowContext $context = null;

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

    public function dataFrameBatchProcessed(Rows $rows, FlowContext $context): void
    {
        $this->totalRowsProcessed += $rows->count();
        $this->memory->capture();

        if ($this->options->collectMetrics) {
            $attributes = [TelemetryAttributes::ATTR_DATAFRAME_NAME => $context->config->name()];
            $this->counterProcessedRows?->add($rows->count(), $attributes);
            $this->throughputRows?->add($rows->count(), $attributes);
        }
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function dataFrameCompleted(FlowContext $context, array $attributes = []): void
    {
        $dataFrameSpan = $this->dataFrameSpan;

        if ($dataFrameSpan === null) {
            return;
        }

        $this->logger()->debug(
            'Data frame processing completed',
            [
                'dataframe_id' => $context->config->id(),
                'dataframe_name' => $context->config->name(),
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
            TelemetryAttributes::ATTR_DATAFRAME_ID => $context->config->id(),
            TelemetryAttributes::ATTR_DATAFRAME_NAME => $context->config->name(),
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

        $this->context = null;
        $this->dataFrameExecutionTime = null;
        $this->totalRowsProcessed = 0;
        $this->dataFrameSpan = null;
        $this->memory = new Consumption();
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function dataFrameFailed(FlowContext $context, Throwable $exception, array $attributes = []): void
    {
        $dataFrameSpan = $this->dataFrameSpan;

        if ($dataFrameSpan === null) {
            return;
        }

        $this->logger->error('Data frame processing failed', [
            'exception' => $exception->getMessage(),
            'dataframe_id' => $context->config->id(),
            'dataframe_name' => $context->config->name(),
        ]);

        $throughput = 0.0;

        if ($this->dataFrameExecutionTime !== null) {
            $durationSeconds = HighResolutionTime::now()->diff($this->dataFrameExecutionTime)->toSeconds();
            $throughput = $durationSeconds > 0 ? round($this->totalRowsProcessed / $durationSeconds, 2) : 0.0;
        }

        $this->tracer->complete(
            $dataFrameSpan
                ->setAttributes(array_merge($attributes, [
                    TelemetryAttributes::ATTR_DATAFRAME_ID => $context->config->id(),
                    TelemetryAttributes::ATTR_DATAFRAME_NAME => $context->config->name(),
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

        $this->context = null;
        $this->dataFrameExecutionTime = null;
        $this->totalRowsProcessed = 0;
        $this->dataFrameSpan = null;
        $this->memory = new Consumption();
    }

    public function dataFrameStarted(FlowContext $context): void
    {
        $this->context = $context;
        $dataFrameSpan = $this->tracer->span(
            'DataFrame ' . $context->config->name(),
            SpanKind::INTERNAL,
            Attributes::create([
                TelemetryAttributes::ATTR_DATAFRAME_ID => $context->config->id(),
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $context->config->name(),
            ]),
        );
        $this->dataFrameSpan = $dataFrameSpan;

        $this->logger()->debug(
            'Data frame processing started',
            [
                'dataframe_id' => $context->config->id(),
                'dataframe_name' => $context->config->name(),
                'cache' => $context->cache()::class,
                'serializer' => $context->config->serializer()::class,
                'optimizers' => array_map(
                    static fn(Optimization $optimization) => $optimization::class,
                    $context->config->optimizer()->optimizations(),
                ),
                'telemetry' => [
                    'trace_loading' => $this->options->traceLoading,
                    'trace_transformations' => $this->options->traceTransformations,
                    'collect_metrics' => $this->options->collectMetrics,
                ],
                'fstab' => array_map(
                    static fn(Filesystem $filesystem) => $filesystem::class,
                    $context->config->fstab()->filesystems(),
                ),
            ],
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
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingCompleted(Loader $loader, array $attributes = []): void
    {
        if ($this->loadingSpan === null) {
            return;
        }
        $this->tracer->complete($this->loadingSpan->setAttributes($attributes));

        $this->loadingSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingFailed(Loader $loader, Throwable $exception, array $attributes = []): void
    {
        if ($this->loadingSpan === null) {
            return;
        }

        $this->logger->error('Loading failed', ['exception' => $exception->getMessage(), 'loader' => $loader::class]);
        $this->tracer->complete(
            $this->loadingSpan
                ->setAttributes($attributes)
                ->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class)
                ->setStatus(SpanStatus::error($exception->getMessage())),
        );

        $this->loadingSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function loadingStarted(Loader $loader, array $attributes = []): void
    {
        if ($this->options->traceLoading === false) {
            return;
        }

        $this->loadingSpan = $this->tracer->span(
            ObjectExtractor::shortName($loader),
            SpanKind::INTERNAL,
            Attributes::create(array_merge([
                TelemetryAttributes::ATTR_LOADER_CLASS => $loader::class,
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->context?->config->name(),
            ], $attributes)),
            parentContext: $this->dataFrameSpan?->context(),
        );
    }

    public function logger(): Logger
    {
        return $this->logger;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationCompleted(Transformer $transformer, array $attributes = []): void
    {
        if ($this->transformationSpan === null) {
            return;
        }

        $this->tracer->complete($this->transformationSpan->setAttributes($attributes));
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationFailed(Transformer $transformer, Throwable $exception, array $attributes = []): void
    {
        if ($this->transformationSpan === null) {
            return;
        }

        $this->logger->error('Transformation failed', [
            'exception' => $exception->getMessage(),
            'transformer' => $transformer::class,
        ]);
        $this->tracer->complete(
            $this->transformationSpan
                ->setAttributes($attributes)
                ->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class)
                ->setStatus(SpanStatus::error($exception->getMessage())),
        );

        $this->transformationSpan = null;
    }

    /**
     * @param TAttributeValueMap $attributes
     */
    public function transformationStarted(Transformer $transformer, array $attributes = []): void
    {
        if (!$this->options->traceTransformations) {
            return;
        }

        $this->transformationSpan = $this->tracer->span(
            ObjectExtractor::shortName($transformer),
            SpanKind::INTERNAL,
            Attributes::create(array_merge([
                TelemetryAttributes::ATTR_TRANSFORMER_CLASS => $transformer::class,
                TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->context?->config->name(),
            ], $attributes)),
            parentContext: $this->dataFrameSpan?->context(),
        );
    }
}
