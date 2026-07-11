<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache;

use Psr\Cache\CacheItemInterface;
use RuntimeException;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Contracts\Cache\TagAwareCacheInterface;

final readonly class FailingTagAwareCacheAdapter implements
    PruneableInterface,
    ResettableInterface,
    TagAwareAdapterInterface,
    TagAwareCacheInterface
{
    public function __construct(
        private string $errorMessage = 'Cache operation failed',
    ) {}

    public function clear(string $prefix = ''): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function commit(): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function delete(string $key): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function deleteItem(mixed $key): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    /**
     * @param array<string> $keys
     */
    public function deleteItems(array $keys): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function get(string $key, callable $callback, ?float $beta = null, ?array &$metadata = null): mixed
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function getItem(mixed $key): CacheItem
    {
        throw new RuntimeException($this->errorMessage);
    }

    /**
     * @param array<string> $keys
     *
     * @return iterable<string, CacheItem>
     */
    public function getItems(array $keys = []): iterable
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function hasItem(mixed $key): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    /**
     * @param array<string> $tags
     */
    public function invalidateTags(array $tags): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function prune(): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function reset(): void
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function save(CacheItemInterface $item): bool
    {
        throw new RuntimeException($this->errorMessage);
    }

    public function saveDeferred(CacheItemInterface $item): bool
    {
        throw new RuntimeException($this->errorMessage);
    }
}
