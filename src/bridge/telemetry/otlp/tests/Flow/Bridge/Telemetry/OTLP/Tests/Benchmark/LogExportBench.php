<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Benchmark;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_log_exporter, otlp_logger_provider};
use function Flow\Telemetry\DSL\batching_log_processor;
use Flow\Bridge\Telemetry\OTLP\Tests\Context\{OtelContext, TransportConfiguration};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Resource;
use PhpBench\Attributes\{Groups, ParamProviders};

#[Groups(['telemetry'])]
final readonly class LogExportBench
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
    public function bench_export_100_logs(array $params) : void
    {
        $config = $params['config'];
        $resource = Resource::empty();
        $transport = $config->createTransport($this->ctx);
        $processor = batching_log_processor(otlp_log_exporter($transport), 100);
        $provider = otlp_logger_provider($processor, new SystemClock());
        $logger = $provider->logger($resource, 'benchmark', '1.0.0');

        for ($i = 0; $i < 100; $i++) {
            $logger->info('Benchmark log message ' . $i, ['iteration' => $i]);
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
