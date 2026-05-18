<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache;

use BadMethodCallException;
use DateTimeImmutable;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Generator;
use Psr\Cache\CacheItemInterface;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Contracts\Cache\ItemInterface;
use Symfony\Contracts\Cache\TagAwareCacheInterface;
use Throwable;

use function count;
use function sprintf;

final readonly class TagAwareTraceableCacheAdapter implements
    PruneableInterface,
    ResettableInterface,
    TagAwareAdapterInterface,
    TagAwareCacheInterface
{
    private Counter $hitCounter;

    private Counter $missCounter;

    private Tracer $tracer;

    public function __construct(
        private TagAwareAdapterInterface $adapter,
        private Telemetry $telemetry,
        private string $poolName,
    ) {
        $this->tracer = $this->telemetry->tracer('flow.symfony.cache', PackageVersion::get('symfony/cache'));
        $meter = $this->telemetry->meter('flow.symfony.cache', PackageVersion::get('symfony/cache'));
        $this->hitCounter = $meter->createCounter('cache.hits', 'operations', 'Number of cache hits');
        $this->missCounter = $meter->createCounter('cache.misses', 'operations', 'Number of cache misses');
    }

    public function clear(string $prefix = ''): bool
    {
        $attributes = [
            'cache.operation' => 'clear',
            'cache.pool' => $this->poolName,
        ];

        if ($prefix !== '') {
            $attributes['cache.prefix'] = $prefix;
        }

        $span = $this->tracer->span("Cache Clear {$this->poolName}", SpanKind::CLIENT, $attributes);

        try {
            $result = $this->adapter->clear($prefix);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function commit(): bool
    {
        $span = $this->tracer->span("Cache Commit {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'commit',
            'cache.pool' => $this->poolName,
        ]);

        try {
            $result = $this->adapter->commit();
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function delete(string $key): bool
    {
        if (!$this->adapter instanceof TagAwareCacheInterface) {
            throw new BadMethodCallException(sprintf(
                'The adapter "%s" does not implement "%s".',
                $this->adapter::class,
                TagAwareCacheInterface::class,
            ));
        }

        $span = $this->tracer->span("Cache Delete {$key} {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'delete',
            'cache.pool' => $this->poolName,
            'cache.key' => $key,
        ]);

        try {
            $result = $this->adapter->delete($key);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function deleteItem(mixed $key): bool
    {
        $span = $this->tracer->span("Cache DeleteItem {$key} {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'deleteItem',
            'cache.pool' => $this->poolName,
            'cache.key' => $key,
        ]);

        try {
            $result = $this->adapter->deleteItem($key);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param array<string> $keys
     */
    public function deleteItems(array $keys): bool
    {
        $span = $this->tracer->span("Cache DeleteItems {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'deleteItems',
            'cache.pool' => $this->poolName,
            'cache.key_count' => count($keys),
        ]);

        try {
            $result = $this->adapter->deleteItems($keys);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param null|array<mixed> $metadata
     */
    public function get(string $key, callable $callback, ?float $beta = null, ?array &$metadata = null): mixed
    {
        if (!$this->adapter instanceof TagAwareCacheInterface) {
            throw new BadMethodCallException(sprintf(
                'The adapter "%s" does not implement "%s".',
                $this->adapter::class,
                TagAwareCacheInterface::class,
            ));
        }

        $hit = true;
        $wrappedCallback = static function (ItemInterface $item, bool &$save) use ($callback, &$hit): mixed {
            $hit = false;

            return $callback($item, $save);
        };

        $result = $this->adapter->get($key, $wrappedCallback, $beta, $metadata);

        if ($hit) {
            $this->hitCounter->add(1, ['cache.pool' => $this->poolName]);
        } else {
            $this->missCounter->add(1, ['cache.pool' => $this->poolName]);
        }

        return $result;
    }

    public function getItem(mixed $key): CacheItem
    {
        $item = $this->adapter->getItem($key);

        if ($item->isHit()) {
            $this->hitCounter->add(1, ['cache.pool' => $this->poolName]);
        } else {
            $this->missCounter->add(1, ['cache.pool' => $this->poolName]);
        }

        return $item;
    }

    /**
     * @param array<string> $keys
     *
     * @return \Generator<string, CacheItem>
     */
    public function getItems(array $keys = []): Generator
    {
        $hits = 0;
        $misses = 0;

        foreach ($this->adapter->getItems($keys) as $key => $item) {
            if ($item->isHit()) {
                $hits++;
            } else {
                $misses++;
            }

            yield $key => $item;
        }

        if ($hits > 0) {
            $this->hitCounter->add($hits, ['cache.pool' => $this->poolName]);
        }

        if ($misses > 0) {
            $this->missCounter->add($misses, ['cache.pool' => $this->poolName]);
        }
    }

    public function hasItem(mixed $key): bool
    {
        $exists = $this->adapter->hasItem($key);

        if ($exists) {
            $this->hitCounter->add(1, ['cache.pool' => $this->poolName]);
        } else {
            $this->missCounter->add(1, ['cache.pool' => $this->poolName]);
        }

        return $exists;
    }

    /**
     * @param array<string> $tags
     */
    public function invalidateTags(array $tags): bool
    {
        $span = $this->tracer->span("Cache InvalidateTags {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'invalidateTags',
            'cache.pool' => $this->poolName,
            'cache.tags' => $tags,
            'cache.tag_count' => count($tags),
        ]);

        try {
            $result = $this->adapter->invalidateTags($tags);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function prune(): bool
    {
        if (!$this->adapter instanceof PruneableInterface) {
            return false;
        }

        $span = $this->tracer->span("Cache Prune {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'prune',
            'cache.pool' => $this->poolName,
        ]);

        try {
            $result = $this->adapter->prune();
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function reset(): void
    {
        if (!$this->adapter instanceof ResettableInterface) {
            return;
        }

        $span = $this->tracer->span("Cache Reset {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'reset',
            'cache.pool' => $this->poolName,
        ]);

        try {
            $this->adapter->reset();
            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function save(CacheItemInterface $item): bool
    {
        $key = $item->getKey();
        $span = $this->tracer->span("Cache Save {$key} {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'save',
            'cache.pool' => $this->poolName,
            'cache.key' => $key,
        ]);

        try {
            $result = $this->adapter->save($item);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function saveDeferred(CacheItemInterface $item): bool
    {
        $key = $item->getKey();
        $span = $this->tracer->span("Cache SaveDeferred {$key} {$this->poolName}", SpanKind::CLIENT, [
            'cache.operation' => 'saveDeferred',
            'cache.pool' => $this->poolName,
            'cache.key' => $key,
        ]);

        try {
            $result = $this->adapter->saveDeferred($item);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }
}
