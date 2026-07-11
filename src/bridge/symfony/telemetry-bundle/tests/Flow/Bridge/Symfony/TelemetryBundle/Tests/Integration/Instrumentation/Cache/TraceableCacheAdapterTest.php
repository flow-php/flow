<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Cache;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\CacheDeferredFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TagAwareTraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\ArrayCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\FailingCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\FailingTagAwareCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\TagAwareArrayCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_map;
use function array_sum;
use function interface_exists;
use function iterator_to_array;

#[CoversClass(TraceableCacheAdapter::class)]
#[CoversClass(TagAwareTraceableCacheAdapter::class)]
#[CoversClass(CacheTelemetryPass::class)]
#[CoversClass(CacheDeferredFlushSubscriber::class)]
final class TraceableCacheAdapterTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(AdapterInterface::class)) {
            self::markTestSkipped('symfony/cache is not installed');
        }

        parent::setUp();
    }

    public function test_all_cache_pools_are_wrapped_when_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container
                        ->register('test.cache.secondary', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
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

        static::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        static::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.secondary'));
    }

    public function test_deferred_flush_subscriber_is_registered_when_flush_deferred_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => [
                            'enabled' => true,
                            'flush_deferred' => true,
                        ],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.telemetry.cache.deferred_flush_subscriber'));
    }

    public function test_deferred_flush_subscriber_is_not_registered_by_default(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => false,
                        'cache' => true,
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.telemetry.cache.deferred_flush_subscriber'));
    }

    public function test_decorator_not_registered_when_feature_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'cache' => false,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.app'));
    }

    public function test_excluded_pool_by_exact_id_is_not_wrapped(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container
                        ->register('test.cache.validator', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
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

        static::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        static::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.validator'));
    }

    public function test_excluded_pool_by_regex_is_not_wrapped(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container
                        ->register('test.cache.profiler.first', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);

                    $container
                        ->register('test.cache.profiler.second', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
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

        static::assertInstanceOf(TraceableCacheAdapter::class, $container->get('test.cache.app'));
        static::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.profiler.first'));
        static::assertInstanceOf(ArrayCacheAdapter::class, $container->get('test.cache.profiler.second'));
    }

    public function test_get_item_records_metrics(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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
        static::assertFalse($item->isHit());

        $cache->get('existing-key', static fn() => 'value');

        $existingItem = $cache->getItem('existing-key');
        static::assertTrue($existingItem->isHit());

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('flow.cache.hits');
        $missMetrics = $processor->metricsWithName('flow.cache.misses');

        $totalHits = array_sum(array_map(static fn($m) => $m->value, $hitMetrics));
        $totalMisses = array_sum(array_map(static fn($m) => $m->value, $missMetrics));

        static::assertSame(1, $totalHits);
        static::assertSame(2, $totalMisses);
        static::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_get_items_records_metrics(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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

        $cache->get('key1', static fn() => 'value1');
        $cache->get('key2', static fn() => 'value2');

        $items = $cache->getItems(['key1', 'key2', 'key3']);
        iterator_to_array($items);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('flow.cache.hits');
        $missMetrics = $processor->metricsWithName('flow.cache.misses');

        $totalHits = array_sum(array_map(static fn($m) => $m->value, $hitMetrics));
        $totalMisses = array_sum(array_map(static fn($m) => $m->value, $missMetrics));

        static::assertSame(2, $totalHits);
        static::assertSame(3, $totalMisses);
    }

    public function test_get_records_hit_metric_on_cache_hit(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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
        $cache->get('my-key', static fn() => 'my-value');
        $cache->get('my-key', static fn() => 'should-not-be-called');

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('flow.cache.hits');
        $missMetrics = $processor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $hitMetrics);
        static::assertCount(1, $missMetrics);

        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));

        static::assertSame(1, $missMetrics[0]->value);
        static::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_get_records_miss_metric_on_cache_miss(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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
        $cache->get('nonexistent-key', static fn() => 'new-value');

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $missMetrics = $processor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $missMetrics);
        static::assertSame(1, $missMetrics[0]->value);
        static::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_has_item_records_hit_metric_when_exists(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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
        $cache->get('existing-key', static fn() => 'value');
        $exists = $cache->hasItem('existing-key');

        static::assertTrue($exists);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $hitMetrics = $processor->metricsWithName('flow.cache.hits');

        static::assertCount(1, $hitMetrics);
        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('test.cache.app', $hitMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_has_item_records_miss_metric_when_not_exists(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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

        static::assertFalse($exists);

        /** @var Telemetry $telemetry */
        $telemetry = $container->get('flow.telemetry');
        $telemetry->flush();

        /** @var MemoryMetricProcessor $processor */
        $processor = $container->get('flow.telemetry.meter_provider.processor');
        $missMetrics = $processor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $missMetrics);
        static::assertSame(1, $missMetrics[0]->value);
        static::assertSame('test.cache.app', $missMetrics[0]->attributes->get('cache.pool'));
    }

    public function test_tag_aware_adapters_are_wrapped_with_tag_aware_traceable(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.tags', TagAwareArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
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

        static::assertInstanceOf(TagAwareTraceableCacheAdapter::class, $container->get('test.cache.tags'));
    }

    public function test_traceable_adapter_records_a_span_for_every_traced_operation(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', ArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
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
        $item = $cache->getItem('save-key');

        $cache->clear();
        $cache->commit();
        $cache->delete('delete-key');
        $cache->deleteItem('delete-item-key');
        $cache->deleteItems(['a', 'b']);
        $cache->prune();
        $cache->reset();
        $cache->save($item);
        $cache->saveDeferred($item);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $operations = array_map(static fn($span) => $span->attributes()['cache.operation'], $processor->endedSpans());

        static::assertSame(
            ['clear', 'commit', 'delete', 'delete_item', 'delete_items', 'prune', 'reset', 'save', 'save_deferred'],
            $operations,
        );
    }

    public function test_traceable_adapter_records_error_span_when_operation_fails(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.app', FailingCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
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
        $item = (new ArrayCacheAdapter())->getItem('save-key');

        $failures = 0;

        foreach ([
            static fn() => $cache->clear(),
            static fn() => $cache->commit(),
            static fn() => $cache->delete('key'),
            static fn() => $cache->deleteItem('key'),
            static fn() => $cache->deleteItems(['a']),
            static fn() => $cache->prune(),
            static fn() => $cache->reset(),
            static fn() => $cache->save($item),
            static fn() => $cache->saveDeferred($item),
        ] as $operation) {
            try {
                $operation();
            } catch (RuntimeException) {
                $failures++;
            }
        }

        static::assertSame(9, $failures);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(9, $spans);

        foreach ($spans as $span) {
            $status = $span->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        }
    }

    public function test_tag_aware_adapter_records_a_span_for_every_traced_operation(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.tags', TagAwareArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
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
        $item = $cache->getItem('save-key');

        $cache->clear();
        $cache->commit();
        $cache->delete('delete-key');
        $cache->deleteItem('delete-item-key');
        $cache->deleteItems(['a', 'b']);
        $cache->invalidateTags(['tag1']);
        $cache->prune();
        $cache->reset();
        $cache->save($item);
        $cache->saveDeferred($item);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $operations = array_map(static fn($span) => $span->attributes()['cache.operation'], $processor->endedSpans());

        static::assertSame(
            [
                'clear',
                'commit',
                'delete',
                'delete_item',
                'delete_items',
                'invalidate_tags',
                'prune',
                'reset',
                'save',
                'save_deferred',
            ],
            $operations,
        );
    }

    public function test_tag_aware_adapter_records_error_span_when_operation_fails(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.tags', FailingTagAwareCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
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
        $item = (new TagAwareArrayCacheAdapter())->getItem('save-key');

        $failures = 0;

        foreach ([
            static fn() => $cache->clear(),
            static fn() => $cache->commit(),
            static fn() => $cache->delete('key'),
            static fn() => $cache->deleteItem('key'),
            static fn() => $cache->deleteItems(['a']),
            static fn() => $cache->invalidateTags(['tag1']),
            static fn() => $cache->prune(),
            static fn() => $cache->reset(),
            static fn() => $cache->save($item),
            static fn() => $cache->saveDeferred($item),
        ] as $operation) {
            try {
                $operation();
            } catch (RuntimeException) {
                $failures++;
            }
        }

        static::assertSame(10, $failures);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(10, $spans);

        foreach ($spans as $span) {
            $status = $span->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        }
    }

    public function test_tag_aware_cache_creates_span_for_invalidate_tags(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container
                        ->register('test.cache.tags', TagAwareArrayCacheAdapter::class)
                        ->addTag('cache.pool')
                        ->setPublic(true);
                });

                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
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

        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.invalidate_tags', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('invalidate_tags', $attributes['cache.operation']);
        static::assertSame('test.cache.tags', $attributes['cache.pool']);
        static::assertSame(['tag1', 'tag2', 'tag3'], $attributes['cache.tags']);
        static::assertSame(3, $attributes['cache.tag_count']);
    }
}
