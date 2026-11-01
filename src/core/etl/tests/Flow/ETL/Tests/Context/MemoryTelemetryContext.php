<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Telemetry\TelemetryContext;
use Flow\ETL\Runtime\Telemetry\TelemetryOptions;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\Filesystem\Path;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\TracerProvider;

use function array_keys;
use function array_map;
use function array_values;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\filesystem_cache;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\filesystem_telemetry_config;
use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\traceable_filesystem;
use function sort;

final class MemoryTelemetryContext
{
    public readonly Config $config;

    public readonly FlowContext $flowContext;

    public readonly MemoryLogProcessor $logs;

    public readonly MemoryMetricProcessor $metrics;

    public readonly MemorySpanProcessor $spans;

    public readonly Telemetry $telemetry;

    public readonly TelemetryContext $telemetryContext;

    public function __construct(
        public readonly TelemetryOptions $options = new TelemetryOptions(),
        ?SortAlgorithmBuilder $sortAlgorithm = null,
    ) {
        $this->spans = new MemorySpanProcessor(new VoidExporter());
        $this->metrics = new MemoryMetricProcessor(new VoidExporter());
        $this->logs = new MemoryLogProcessor(new VoidExporter());

        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        $this->telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($this->spans, $clock, $contextStorage),
            new MeterProvider($this->metrics, $clock),
            new LoggerProvider($this->logs, $clock, $contextStorage),
        );

        $configBuilder = config_builder()->withTelemetry($this->telemetry, $this->options);

        if ($sortAlgorithm !== null) {
            $configBuilder->sort($sortAlgorithm);
        }

        $this->config = $configBuilder->build();
        $this->flowContext = flow_context($this->config);
        $this->telemetryContext = new TelemetryContext(
            $this->config->id(),
            $this->config->name(),
            $this->config->backend(),
            $this->telemetry->logger('flow-php'),
            $this->telemetry->tracer('flow-php'),
            $this->telemetry->meter('flow-php'),
            $this->options,
        );
    }

    public function traceableStorage(Path $cacheDir): FilesystemRowsStorage
    {
        return filesystem_cache($cacheDir, traceable_filesystem(native_local_filesystem(), filesystem_telemetry_config(
            $this->telemetry,
            new FakeClock(),
            filesystem_telemetry_options(collect_metrics: false),
        )));
    }

    /**
     * What was emitted, without the values that change between runs (times, ids, durations): every ended span's name,
     * kind, status and attribute keys, every log's severity, body and attribute keys, every metric's name, type and
     * attribute keys, each in emission order.
     *
     * @return array{spans: list<array{string, string, ?string, list<string>}>, logs: list<array{int, string, list<string>}>, metrics: list<array{string, string, list<string>}>}
     */
    public function signals(): array
    {
        $this->telemetry->flush();

        return [
            'spans' => array_values(array_map(static fn(Span $span): array => [
                $span->name(),
                $span->kind()->name,
                $span->status()?->code->name,
                self::keys($span->attributes()),
            ], $this->spans->endedSpans())),
            'logs' => array_values(array_map(static fn(LogEntry $entry): array => [
                $entry->normalize()['record']['severity'],
                $entry->normalize()['record']['body'],
                self::keys($entry->normalize()['record']['attributes']),
            ], $this->logs->entries())),
            'metrics' => array_values(array_map(static fn(Metric $metric): array => [
                $metric->normalize()['name'],
                $metric->normalize()['type'],
                self::keys($metric->normalize()['attributes']),
            ], $this->metrics->metrics())),
        ];
    }

    /**
     * @param array<string, mixed> $attributes
     *
     * @return list<string>
     */
    public static function keys(array $attributes): array
    {
        $keys = array_keys($attributes);
        sort($keys);

        return $keys;
    }
}
