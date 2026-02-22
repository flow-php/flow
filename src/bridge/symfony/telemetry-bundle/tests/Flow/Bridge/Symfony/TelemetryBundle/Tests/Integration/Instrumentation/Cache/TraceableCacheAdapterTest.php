<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Cache;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\{TagAwareTraceableCacheAdapter, TraceableCacheAdapter};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\{ArrayCacheAdapter, TagAwareArrayCacheAdapter};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\{MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

#[CoversClass(TraceableCacheAdapter::class)]
#[CoversClass(TagAwareTraceableCacheAdapter::class)]
#[CoversClass(CacheTelemetryPass::class)]
final class TraceableCacheAdapterTest extends KernelTestCase
{
    protected function setUp() : void
    {
        if (!\interface_exists(AdapterInterface::class)) {
            self::markTestSkipped('symfony/cache is not installed');
        }

        parent::setUp();
    }

    public function test_all_cache_pools_are_wrapped_when_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container->register('test.cache.secondary', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        self::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.secondary'));
    }

    public function test_decorator_not_registered_when_feature_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'cache' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.app'));
    }

    public function test_excluded_pool_by_exact_id_is_not_wrapped() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container->register('test.cache.validator', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => [
                            'enabled' => true,
                            'exclude_pools' => ['test.cache.validator'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        self::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.validator'));
    }

    public function test_excluded_pool_by_regex_is_not_wrapped() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container->register('test.cache.profiler.first', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container->register('test.cache.profiler.second', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => [
                            'enabled' => true,
                            'exclude_pools' => ['/^test\\.cache\\.profiler\\..*/'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        self::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.profiler.first'));
        self::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.profiler.second'));
    }

    public function test_get_item_records_metrics() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');

        $item = $cache->getItem('missing-key');
        self::assertFalse($item->isHit());

        $cache->get('existing-key', static fn () => 'value');

        $existingItem = $cache->getItem('existing-key');
        self::assertTrue($existingItem->isHit());

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('cache.hits');
        $missMetrics = $processor->metricsWithName('cache.misses');

        $totalHits = \array_sum(\array_map(static fn ($m) => $m->value, $hitMetrics));
        $totalMisses = \array_sum(\array_map(static fn ($m) => $m->value, $missMetrics));

        self::assertSame(1, $totalHits);
        self::assertSame(2, $totalMisses);
        self::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_get_items_records_metrics() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');

        $cache->get('key1', static fn () => 'value1');
        $cache->get('key2', static fn () => 'value2');

        $items = $cache->getItems(['key1', 'key2', 'key3']);
        \iterator_to_array($items);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('cache.hits');
        $missMetrics = $processor->metricsWithName('cache.misses');

        $totalHits = \array_sum(\array_map(static fn ($m) => $m->value, $hitMetrics));
        $totalMisses = \array_sum(\array_map(static fn ($m) => $m->value, $missMetrics));

        self::assertSame(2, $totalHits);
        self::assertSame(3, $totalMisses);
    }

    public function test_get_records_hit_metric_on_cache_hit() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('my-key', static fn () => 'my-value');
        $cache->get('my-key', static fn () => 'should-not-be-called');

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('cache.hits');
        $missMetrics = $processor->metricsWithName('cache.misses');

        self::assertCount(1, $hitMetrics);
        self::assertCount(1, $missMetrics);

        self::assertSame(1, $hitMetrics[0]->value);
        self::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));

        self::assertSame(1, $missMetrics[0]->value);
        self::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_get_records_miss_metric_on_cache_miss() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('nonexistent-key', static fn () => 'new-value');

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $missMetrics = $processor->metricsWithName('cache.misses');

        self::assertCount(1, $missMetrics);
        self::assertSame(1, $missMetrics[0]->value);
        self::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_has_item_records_hit_metric_when_exists() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('existing-key', static fn () => 'value');
        $exists = $cache->hasItem('existing-key');

        self::assertTrue($exists);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('cache.hits');

        self::assertCount(1, $hitMetrics);
        self::assertSame(1, $hitMetrics[0]->value);
        self::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_has_item_records_miss_metric_when_not_exists() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $exists = $cache->hasItem('nonexistent-key');

        self::assertFalse($exists);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $missMetrics = $processor->metricsWithName('cache.misses');

        self::assertCount(1, $missMetrics);
        self::assertSame(1, $missMetrics[0]->value);
        self::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_tag_aware_adapters_are_wrapped_with_tag_aware_traceable() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.tags', TagAwareArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(TagAwareTraceableCacheAdapter::class, $container->get('test.cache.tags'));
    }

    public function test_tag_aware_cache_creates_span_for_invalidate_tags() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.tags', TagAwareArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var TagAwareTraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.tags');
        $cache->invalidateTags(['tag1', 'tag2', 'tag3']);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache InvalidateTags test.cache.tags', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('invalidateTags', $attributes['cache.operation']);
        self::assertSame('test.cache.tags', $attributes['cache.pool']);
        self::assertSame(['tag1', 'tag2', 'tag3'], $attributes['cache.tags']);
        self::assertSame(3, $attributes['cache.tag_count']);
    }
}
