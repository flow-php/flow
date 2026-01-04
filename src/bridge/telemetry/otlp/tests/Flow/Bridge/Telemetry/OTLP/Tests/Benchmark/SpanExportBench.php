<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Benchmark;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_span_exporter, otlp_tracer_provider};
use function Flow\Telemetry\DSL\batching_span_processor;
use Flow\Bridge\Telemetry\OTLP\Tests\Context\{OtelContext, TransportConfiguration};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Resource;
use PhpBench\Attributes\{Groups, ParamProviders};

#[Groups(['telemetry'])]
final readonly class SpanExportBench
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
    public function bench_export_1k_spans(array $params) : void
    {
        $config = $params['config'];
        $resource = Resource::empty();
        $transport = $config->createTransport($this->ctx);
        $processor = batching_span_processor(otlp_span_exporter($transport), 100);
        $provider = otlp_tracer_provider($processor, new SystemClock());
        $tracer = $provider->tracer($resource, 'benchmark', '1.0.0');

        for ($i = 0; $i < 1000; $i++) {
            $span = $tracer->span('bench-span-' . $i);
            $span->setAttribute('iteration', $i);
            $tracer->complete($span);
        }
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
