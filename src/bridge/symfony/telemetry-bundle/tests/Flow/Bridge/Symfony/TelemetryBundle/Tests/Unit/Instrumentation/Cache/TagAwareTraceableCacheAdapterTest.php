<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Cache;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TagAwareTraceableCacheAdapter;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogProcessor, VoidMetricProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, TracerProvider};
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Cache\CacheItemInterface;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\Cache\{CacheItem, PruneableInterface, ResettableInterface};

#[CoversClass(TagAwareTraceableCacheAdapter::class)]
final class TagAwareTraceableCacheAdapterTest extends TestCase
{
    public function test_clear_creates_span_with_correct_attributes() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->clear();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('cache.clear', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());
        self::assertSame('clear', $span->attributes()['cache.operation']);
        self::assertSame('test.pool', $span->attributes()['cache.pool']);
        self::assertArrayNotHasKey('cache.prefix', $span->attributes());
    }

    public function test_clear_with_prefix_includes_prefix_attribute() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->clear('app_');

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('app_', $span->attributes()['cache.prefix']);
    }

    public function test_delete_throws_when_adapter_does_not_implement_tag_aware_cache_interface() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $this->expectException(\BadMethodCallException::class);
        $this->expectExceptionMessage('does not implement');

        $traceable->delete('key');
    }

    public function test_get_throws_when_adapter_does_not_implement_tag_aware_cache_interface() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $this->expectException(\BadMethodCallException::class);
        $this->expectExceptionMessage('does not implement');

        $traceable->get('key', static fn () => 'value');
    }

    public function test_invalidate_tags_creates_span_with_tag_count() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->invalidateTags(['tag1', 'tag2', 'tag3']);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('cache.invalidate_tags', $span->name());
        self::assertSame(['tag1', 'tag2', 'tag3'], $span->attributes()['cache.tags']);
        self::assertSame(3, $span->attributes()['cache.tag_count']);
    }

    public function test_operation_records_exception_on_failure() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                throw new \RuntimeException('Connection lost');
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $exceptionThrown = false;

        try {
            $traceable->clear();
        } catch (\RuntimeException) {
            $exceptionThrown = true;
        }

        self::assertTrue($exceptionThrown);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $events = $spans[0]->events();
        self::assertCount(1, $events);
        self::assertSame('exception', $events[0]->name());
    }

    public function test_operation_sets_error_status_on_failure() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                throw new \RuntimeException('Connection lost');
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        try {
            $traceable->clear();
        } catch (\RuntimeException) {
        }

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $status = $spans[0]->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertSame('Connection lost', $status->description);
    }

    public function test_prune_delegates_to_adapter_when_pruneable() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements PruneableInterface, TagAwareAdapterInterface {
            public bool $pruneCalled = false;

            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function prune() : bool
            {
                $this->pruneCalled = true;

                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $result = $traceable->prune();

        self::assertTrue($result);
        self::assertTrue($adapter->pruneCalled);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_prune_returns_false_when_adapter_is_not_pruneable() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $result = $traceable->prune();

        self::assertFalse($result);
        self::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_reset_delegates_to_adapter_when_resettable() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements ResettableInterface, TagAwareAdapterInterface {
            public bool $resetCalled = false;

            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function reset() : void
            {
                $this->resetCalled = true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->reset();

        self::assertTrue($adapter->resetCalled);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_reset_does_nothing_when_adapter_is_not_resettable() : void
    {
        $spanProcessor = new MemorySpanProcessor(new MemorySpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = '') : bool
            {
                return true;
            }

            public function commit() : bool
            {
                return true;
            }

            public function deleteItem(mixed $key) : bool
            {
                return true;
            }

            public function deleteItems(array $keys) : bool
            {
                return true;
            }

            public function getItem(mixed $key) : CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []) : iterable
            {
                return [];
            }

            public function hasItem(mixed $key) : bool
            {
                return false;
            }

            public function invalidateTags(array $tags) : bool
            {
                return true;
            }

            public function save(CacheItemInterface $item) : bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item) : bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->reset();

        self::assertCount(0, $spanProcessor->endedSpans());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor) : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
