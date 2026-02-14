<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache;

use Flow\Telemetry\{PackageVersion, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, Tracer};
use Psr\Cache\CacheItemInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\Cache\{CacheItem, PruneableInterface, ResettableInterface};
use Symfony\Contracts\Cache\{CacheInterface, ItemInterface};

final readonly class TraceableCacheAdapter implements AdapterInterface, CacheInterface, PruneableInterface, ResettableInterface
{
    private Tracer $tracer;

    public function __construct(
        private AdapterInterface $adapter,
        private Telemetry $telemetry,
        private string $poolName,
    ) {
        $this->tracer = $this->telemetry->tracer('flow.symfony.cache', PackageVersion::get('symfony/cache'));
    }

    public function clear(string $prefix = '') : bool
    {
        $attributes = [
            'cache.operation' => 'clear',
            'cache.pool' => $this->poolName,
        ];

        if ($prefix !== '') {
            $attributes['cache.prefix'] = $prefix;
        }

        $span = $this->tracer->span('cache.clear', SpanKind::CLIENT, $attributes);

        try {
            $result = $this->adapter->clear($prefix);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function commit() : bool
    {
        $span = $this->tracer->span(
            'cache.commit',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'commit',
                'cache.pool' => $this->poolName,
            ]
        );

        try {
            $result = $this->adapter->commit();
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function delete(string $key) : bool
    {
        if (!$this->adapter instanceof CacheInterface) {
            throw new \BadMethodCallException(\sprintf('The adapter "%s" does not implement "%s".', $this->adapter::class, CacheInterface::class));
        }

        $span = $this->tracer->span(
            'cache.delete',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'delete',
                'cache.pool' => $this->poolName,
                'cache.key' => $key,
            ]
        );

        try {
            $result = $this->adapter->delete($key);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function deleteItem(mixed $key) : bool
    {
        $span = $this->tracer->span(
            'cache.delete_item',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'deleteItem',
                'cache.pool' => $this->poolName,
                'cache.key' => $key,
            ]
        );

        try {
            $result = $this->adapter->deleteItem($key);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param array<string> $keys
     */
    public function deleteItems(array $keys) : bool
    {
        $span = $this->tracer->span(
            'cache.delete_items',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'deleteItems',
                'cache.pool' => $this->poolName,
                'cache.key_count' => \count($keys),
            ]
        );

        try {
            $result = $this->adapter->deleteItems($keys);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param null|array<mixed> $metadata
     */
    public function get(string $key, callable $callback, ?float $beta = null, ?array &$metadata = null) : mixed
    {
        if (!$this->adapter instanceof CacheInterface) {
            throw new \BadMethodCallException(\sprintf('The adapter "%s" does not implement "%s".', $this->adapter::class, CacheInterface::class));
        }

        $span = $this->tracer->span(
            'cache.get',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'get',
                'cache.pool' => $this->poolName,
                'cache.key' => $key,
            ]
        );

        $hit = true;
        $wrappedCallback = static function (ItemInterface $item, bool &$save) use ($callback, &$hit) : mixed {
            $hit = false;

            return $callback($item, $save);
        };

        try {
            $result = $this->adapter->get($key, $wrappedCallback, $beta, $metadata);
            $span->setAttribute('cache.hit', $hit);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function getItem(mixed $key) : CacheItem
    {
        $span = $this->tracer->span(
            'cache.get_item',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'getItem',
                'cache.pool' => $this->poolName,
                'cache.key' => $key,
            ]
        );

        try {
            $item = $this->adapter->getItem($key);
            $span->setAttribute('cache.hit', $item->isHit());
            $span->setStatus(SpanStatus::ok());

            return $item;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param array<string> $keys
     *
     * @return iterable<string, CacheItem>
     */
    public function getItems(array $keys = []) : iterable
    {
        $span = $this->tracer->span(
            'cache.get_items',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'getItems',
                'cache.pool' => $this->poolName,
                'cache.key_count' => \count($keys),
            ]
        );

        try {
            $items = $this->adapter->getItems($keys);
            $itemsArray = \iterator_to_array($items);

            $hits = 0;
            $misses = 0;

            foreach ($itemsArray as $item) {
                if ($item->isHit()) {
                    $hits++;
                } else {
                    $misses++;
                }
            }

            $span->setAttribute('cache.hits', $hits);
            $span->setAttribute('cache.misses', $misses);
            $span->setStatus(SpanStatus::ok());

            return $itemsArray;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function hasItem(mixed $key) : bool
    {
        $span = $this->tracer->span(
            'cache.has_item',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'hasItem',
                'cache.pool' => $this->poolName,
                'cache.key' => $key,
            ]
        );

        try {
            $exists = $this->adapter->hasItem($key);
            $span->setAttribute('cache.exists', $exists);
            $span->setStatus(SpanStatus::ok());

            return $exists;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function prune() : bool
    {
        if (!$this->adapter instanceof PruneableInterface) {
            return false;
        }

        $span = $this->tracer->span(
            'cache.prune',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'prune',
                'cache.pool' => $this->poolName,
            ]
        );

        try {
            $result = $this->adapter->prune();
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function reset() : void
    {
        if (!$this->adapter instanceof ResettableInterface) {
            return;
        }

        $span = $this->tracer->span(
            'cache.reset',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'reset',
                'cache.pool' => $this->poolName,
            ]
        );

        try {
            $this->adapter->reset();
            $span->setStatus(SpanStatus::ok());
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function save(CacheItemInterface $item) : bool
    {
        $span = $this->tracer->span(
            'cache.save',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'save',
                'cache.pool' => $this->poolName,
                'cache.key' => $item->getKey(),
            ]
        );

        try {
            $result = $this->adapter->save($item);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function saveDeferred(CacheItemInterface $item) : bool
    {
        $span = $this->tracer->span(
            'cache.save_deferred',
            SpanKind::CLIENT,
            [
                'cache.operation' => 'saveDeferred',
                'cache.pool' => $this->poolName,
                'cache.key' => $item->getKey(),
            ]
        );

        try {
            $result = $this->adapter->saveDeferred($item);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }
}
