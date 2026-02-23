<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\{InMemoryCache, TraceableCache};
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidMetricExporter, VoidSpanExporter};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\TracerProvider;

final class TraceableCacheTestSuite extends CacheBaseTestSuite
{
    private InMemoryCache $innerCache;

    private Telemetry $telemetry;

    #[\Override]
    protected function setUp() : void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $spanProcessor = new MemorySpanProcessor(new VoidSpanExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidMetricExporter());
        $logProcessor = new MemoryLogProcessor(new VoidLogExporter());

        $this->telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $this->innerCache = new InMemoryCache();

        parent::setUp();
    }

    protected function cache() : Cache
    {
        return new TraceableCache($this->innerCache, $this->telemetry);
    }
}
