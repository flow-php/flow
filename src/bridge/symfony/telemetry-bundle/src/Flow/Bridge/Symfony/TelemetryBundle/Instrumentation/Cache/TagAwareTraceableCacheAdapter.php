<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache;

use BadMethodCallException;
use DateTimeImmutable;
use Flow\Telemetry\CacheAttributes;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
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
use function is_string;
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
        $this->hitCounter = $meter->createCounter('flow.cache.hits', '{operation}', 'Number of cache hits');
        $this->missCounter = $meter->createCounter('flow.cache.misses', '{operation}', 'Number of cache misses');
    }

    public function clear(string $prefix = ''): bool
    {
        $attributes = [
            CacheAttributes::CACHE_OPERATION => 'clear',
            CacheAttributes::CACHE_POOL => $this->poolName,
        ];

        if ($prefix !== '') {
            $attributes[CacheAttributes::CACHE_PREFIX] = $prefix;
        }

        $span = $this->tracer->span('cache.clear', SpanKind::CLIENT, $attributes);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->clear($prefix);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function commit(): bool
    {
        $span = $this->tracer->span('cache.commit', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'commit',
            CacheAttributes::CACHE_POOL => $this->poolName,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->commit();
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
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

        $span = $this->tracer->span('cache.delete', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'delete',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_KEY => $key,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->delete($key);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function deleteItem(mixed $key): bool
    {
        $keyString = is_string($key) ? $key : (string) $key;

        $span = $this->tracer->span('cache.delete_item', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'delete_item',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_KEY => $keyString,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->deleteItem($keyString);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    /**
     * @param array<string> $keys
     */
    public function deleteItems(array $keys): bool
    {
        $span = $this->tracer->span('cache.delete_items', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'delete_items',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_KEY_COUNT => count($keys),
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->deleteItems($keys);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
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
            $this->hitCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
        } else {
            $this->missCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
        }

        return $result;
    }

    public function getItem(mixed $key): CacheItem
    {
        $item = $this->adapter->getItem($key);

        if ($item->isHit()) {
            $this->hitCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
        } else {
            $this->missCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
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
            $this->hitCounter->add($hits, [CacheAttributes::CACHE_POOL => $this->poolName]);
        }

        if ($misses > 0) {
            $this->missCounter->add($misses, [CacheAttributes::CACHE_POOL => $this->poolName]);
        }
    }

    public function hasItem(mixed $key): bool
    {
        $keyString = is_string($key) ? $key : (string) $key;
        $exists = $this->adapter->hasItem($keyString);

        if ($exists) {
            $this->hitCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
        } else {
            $this->missCounter->add(1, [CacheAttributes::CACHE_POOL => $this->poolName]);
        }

        return $exists;
    }

    /**
     * @param array<string> $tags
     */
    public function invalidateTags(array $tags): bool
    {
        $span = $this->tracer->span('cache.invalidate_tags', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'invalidate_tags',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_TAGS => $tags,
            CacheAttributes::CACHE_TAG_COUNT => count($tags),
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->invalidateTags($tags);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function prune(): bool
    {
        if (!$this->adapter instanceof PruneableInterface) {
            return false;
        }

        $span = $this->tracer->span('cache.prune', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'prune',
            CacheAttributes::CACHE_POOL => $this->poolName,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->prune();
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function reset(): void
    {
        if (!$this->adapter instanceof ResettableInterface) {
            return;
        }

        $span = $this->tracer->span('cache.reset', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'reset',
            CacheAttributes::CACHE_POOL => $this->poolName,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            $this->adapter->reset();
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function save(CacheItemInterface $item): bool
    {
        $key = $item->getKey();
        $span = $this->tracer->span('cache.save', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'save',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_KEY => $key,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->save($item);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function saveDeferred(CacheItemInterface $item): bool
    {
        $key = $item->getKey();
        $span = $this->tracer->span('cache.save_deferred', SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'save_deferred',
            CacheAttributes::CACHE_POOL => $this->poolName,
            CacheAttributes::CACHE_KEY => $key,
        ]);
        $scope = $this->tracer->activate($span);

        try {
            return $this->adapter->saveDeferred($item);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }
}
