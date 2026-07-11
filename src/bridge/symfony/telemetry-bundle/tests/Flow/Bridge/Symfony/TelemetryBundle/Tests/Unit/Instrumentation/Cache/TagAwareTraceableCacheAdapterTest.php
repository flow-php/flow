<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Cache;

use BadMethodCallException;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TagAwareTraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Cache\CacheItemInterface;
use RuntimeException;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;

#[CoversClass(TagAwareTraceableCacheAdapter::class)]
final class TagAwareTraceableCacheAdapterTest extends TestCase
{
    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $innerAdapter = $this->createMock(TagAwareAdapterInterface::class);
        $innerAdapter->method('deleteItem')->willReturn(true);
        $innerAdapter->method('save')->willReturn(true);
        $adapter = new TagAwareTraceableCacheAdapter(
            $innerAdapter,
            TelemetryMother::suppressed($spanProcessor),
            'test.pool',
        );

        $adapter->clear();
        $adapter->deleteItem('key');
        $adapter->save($this->createMock(CacheItemInterface::class));

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'cache instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_clear_creates_span_with_correct_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->clear();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.clear', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());
        static::assertSame('clear', $span->attributes()['cache.operation']);
        static::assertSame('test.pool', $span->attributes()['cache.pool']);
        static::assertArrayNotHasKey('cache.prefix', $span->attributes());
    }

    public function test_clear_with_prefix_includes_prefix_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->clear('app_');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('app_', $span->attributes()['cache.prefix']);
    }

    public function test_delete_throws_when_adapter_does_not_implement_tag_aware_cache_interface(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('does not implement');

        $traceable->delete('key');
    }

    public function test_get_throws_when_adapter_does_not_implement_tag_aware_cache_interface(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('does not implement');

        $traceable->get('key', static fn() => 'value');
    }

    public function test_invalidate_tags_creates_span_with_tag_count(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->invalidateTags(['tag1', 'tag2', 'tag3']);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.invalidate_tags', $span->name());
        static::assertSame(['tag1', 'tag2', 'tag3'], $span->attributes()['cache.tags']);
        static::assertSame(3, $span->attributes()['cache.tag_count']);
    }

    public function test_operation_records_exception_on_failure(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                throw new RuntimeException('Connection lost');
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $exceptionThrown = false;

        try {
            $traceable->clear();
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $events = $spans[0]->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());
    }

    public function test_operation_sets_error_status_on_failure(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                throw new RuntimeException('Connection lost');
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        try {
            $traceable->clear();
        } catch (RuntimeException) {
        }

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Connection lost', $status->description);
    }

    public function test_prune_delegates_to_adapter_when_pruneable(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements PruneableInterface, TagAwareAdapterInterface {
            public bool $pruneCalled = false;

            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function prune(): bool
            {
                $this->pruneCalled = true;

                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $result = $traceable->prune();

        static::assertTrue($result);
        static::assertTrue($adapter->pruneCalled);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_prune_returns_false_when_adapter_is_not_pruneable(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $result = $traceable->prune();

        static::assertFalse($result);
        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_reset_delegates_to_adapter_when_resettable(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements ResettableInterface, TagAwareAdapterInterface {
            public bool $resetCalled = false;

            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function reset(): void
            {
                $this->resetCalled = true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->reset();

        static::assertTrue($adapter->resetCalled);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_reset_does_nothing_when_adapter_is_not_resettable(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $adapter = new class implements TagAwareAdapterInterface {
            public function clear(string $prefix = ''): bool
            {
                return true;
            }

            public function commit(): bool
            {
                return true;
            }

            public function deleteItem(mixed $key): bool
            {
                return true;
            }

            public function deleteItems(array $keys): bool
            {
                return true;
            }

            public function getItem(mixed $key): CacheItem
            {
                return new CacheItem();
            }

            public function getItems(array $keys = []): iterable
            {
                return [];
            }

            public function hasItem(mixed $key): bool
            {
                return false;
            }

            public function invalidateTags(array $tags): bool
            {
                return true;
            }

            public function save(CacheItemInterface $item): bool
            {
                return true;
            }

            public function saveDeferred(CacheItemInterface $item): bool
            {
                return true;
            }
        };

        $traceable = new TagAwareTraceableCacheAdapter($adapter, $telemetry, 'test.pool');

        $traceable->reset();

        static::assertCount(0, $spanProcessor->endedSpans());
    }
}
