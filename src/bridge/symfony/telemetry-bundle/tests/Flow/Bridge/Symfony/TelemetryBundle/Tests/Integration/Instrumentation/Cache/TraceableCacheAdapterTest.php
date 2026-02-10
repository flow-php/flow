<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Cache;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\{TagAwareTraceableCacheAdapter, TraceableCacheAdapter};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\{ArrayCacheAdapter, FailingCacheAdapter, TagAwareArrayCacheAdapter};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
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

    public function test_get_item_records_hit_miss() : void
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');

        $item = $cache->getItem('missing-key');
        self::assertFalse($item->isHit());

        $cache->get('existing-key', static fn () => 'value');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $processor->endedSpans();

        $existingItem = $cache->getItem('existing-key');
        self::assertTrue($existingItem->isHit());

        $spans = $processor->endedSpans();
        $getItemSpan = $spans[\count($spans) - 1];

        self::assertSame('cache getItem', $getItemSpan->name());
        self::assertTrue($getItemSpan->attributes()['cache.hit']);
    }

    public function test_get_items_records_hits_and_misses() : void
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');

        $cache->get('key1', static fn () => 'value1');
        $cache->get('key2', static fn () => 'value2');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $processor->endedSpans();

        $items = $cache->getItems(['key1', 'key2', 'key3']);
        \iterator_to_array($items);

        $spans = $processor->endedSpans();
        $getItemsSpan = $spans[\count($spans) - 1];

        self::assertSame('cache getItems', $getItemsSpan->name());
        self::assertSame(3, $getItemsSpan->attributes()['cache.key_count']);
        self::assertSame(2, $getItemsSpan->attributes()['cache.hits']);
        self::assertSame(1, $getItemsSpan->attributes()['cache.misses']);
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
        self::assertSame('cache invalidateTags', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('invalidateTags', $attributes['cache.operation']);
        self::assertSame('test.cache.tags', $attributes['cache.pool']);
        self::assertSame(['tag1', 'tag2', 'tag3'], $attributes['cache.tags']);
        self::assertSame(3, $attributes['cache.tag_count']);
    }

    public function test_wrapped_cache_creates_span_for_get_operation() : void
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('my-key', static fn () => 'my-value');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('cache get', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('get', $attributes['cache.operation']);
        self::assertSame('test.cache.app', $attributes['cache.pool']);
        self::assertSame('my-key', $attributes['cache.key']);
    }

    public function test_wrapped_cache_records_cache_hit() : void
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('my-key', static fn () => 'my-value');
        $cache->get('my-key', static fn () => 'should-not-be-called');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(2, $spans);

        $firstSpan = $spans[0];
        self::assertFalse($firstSpan->attributes()['cache.hit']);

        $secondSpan = $spans[1];
        self::assertTrue($secondSpan->attributes()['cache.hit']);
    }

    public function test_wrapped_cache_records_cache_miss() : void
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.app');
        $cache->get('nonexistent-key', static fn () => 'new-value');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);
        self::assertFalse($spans[0]->attributes()['cache.hit']);
    }

    public function test_wrapped_cache_records_exception() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('test.cache.failing', FailingCacheAdapter::class)
                        ->addArgument('Connection refused')
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

        /** @var TraceableCacheAdapter $cache */
        $cache = $container->get('test.cache.failing');

        $exceptionThrown = false;

        try {
            $cache->get('my-key', static fn () => 'value');
        } catch (\RuntimeException $e) {
            $exceptionThrown = true;
            self::assertSame('Connection refused', $e->getMessage());
        }

        self::assertTrue($exceptionThrown, 'Expected exception was not thrown');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertSame('Connection refused', $status->description);

        $events = $span->events();
        self::assertCount(1, $events);
        self::assertSame('exception', $events[0]->name());
    }
}
