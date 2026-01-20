<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Benchmark;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_meter_provider, otlp_metric_exporter};
use function Flow\Telemetry\DSL\batching_metric_processor;
use Flow\Bridge\Telemetry\OTLP\Tests\Context\{OtelContext, TransportConfiguration};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Resource;
use PhpBench\Attributes\{Groups, ParamProviders};

#[Groups(['telemetry'])]
final readonly class MetricExportBench
{
    private OtelContext $ctx;

    public function __construct()
    {
        $this->ctx = OtelContext::instance();
    }

    /**
     * @param array{config: TransportConfiguration} $params
     */
    #[ParamProviders('provideTransports')]
    public function bench_export_100_counter_increments(array $params) : void
    {
        $config = $params['config'];
        $resource = Resource::empty();
        $transport = $config->createTransport($this->ctx);
        $processor = batching_metric_processor(otlp_metric_exporter($transport), 100);
        $provider = otlp_meter_provider($processor, new SystemClock());
        $meter = $provider->meter($resource, 'benchmark', '1.0.0');
        $counter = $meter->createCounter('bench.counter');

        for ($i = 0; $i < 100; $i++) {
            $counter->add(1, ['iteration' => $i]);
        }

        $transport->shutdown();
    }

    /**
     * @param array{config: TransportConfiguration} $params
     */
    #[ParamProviders('provideTransports')]
    public function bench_export_100_histogram_records(array $params) : void
    {
        $config = $params['config'];
        $resource = Resource::empty();
        $transport = $config->createTransport($this->ctx);
        $processor = batching_metric_processor(otlp_metric_exporter($transport), 100);
        $provider = otlp_meter_provider($processor, new SystemClock());
        $meter = $provider->meter($resource, 'benchmark', '1.0.0');
        $histogram = $meter->createHistogram('bench.histogram', 'ms');

        for ($i = 0; $i < 100; $i++) {
            $histogram->record((float) ($i % 100), ['iteration' => $i]);
        }

        $transport->shutdown();
    }

    /**
     * @return \Generator<string, array{config: TransportConfiguration}>
     */
    public function provideTransports() : \Generator
    {
        foreach (TransportConfiguration::available() as $config) {
            yield $config->name => ['config' => $config];
        }
    }
}
