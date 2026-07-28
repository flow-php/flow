<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\Clock\FakeClock;
use Flow\ETL\Config;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Config\Telemetry\TelemetryContext;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\FlowContext;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\TracerProvider;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;

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
            $this->telemetry->logger('flow-php'),
            $this->telemetry->tracer('flow-php'),
            $this->telemetry->meter('flow-php'),
            $this->options,
        );
    }
}
