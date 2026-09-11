<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache;

use Psr\Cache\CacheItemInterface;
use ReflectionClass;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Contracts\Cache\TagAwareCacheInterface;

use function array_flip;
use function array_keys;
use function str_starts_with;

final class TagAwareArrayCacheAdapter implements
    PruneableInterface,
    ResettableInterface,
    TagAwareAdapterInterface,
    TagAwareCacheInterface
{
    /** @var array<string, mixed> */
    private array $cache = [];

    /** @var array<string, CacheItem> */
    private array $deferred = [];

    /** @var array<string, array<string>> */
    private array $tags = [];

    public function clear(string $prefix = ''): bool
    {
        if ($prefix === '') {
            $this->cache = [];
            $this->tags = [];

            return true;
        }

        foreach (array_keys($this->cache) as $key) {
            if (str_starts_with($key, $prefix)) {
                unset($this->cache[$key], $this->tags[$key]);
            }
        }

        return true;
    }

    public function commit(): bool
    {
        foreach ($this->deferred as $item) {
            $this->save($item);
        }
        $this->deferred = [];

        return true;
    }

    public function delete(string $key): bool
    {
        unset($this->cache[$key], $this->tags[$key]);

        return true;
    }

    public function deleteItem(mixed $key): bool
    {
        unset($this->cache[$key], $this->tags[$key]);

        return true;
    }

    /**
     * @param array<string> $keys
     */
    public function deleteItems(array $keys): bool
    {
        foreach ($keys as $key) {
            unset($this->cache[$key], $this->tags[$key]);
        }

        return true;
    }

    public function get(string $key, callable $callback, ?float $beta = null, ?array &$metadata = null): mixed
    {
        if (isset($this->cache[$key])) {
            // a cache hands back whatever was stored under the key
            // @mago-ignore analysis:mixed-return-statement
            return $this->cache[$key];
        }

        $item = $this->getItem($key);
        $save = true;
        $value = $callback($item, $save);

        if ($save) {
            $this->cache[$key] = $value;
        }

        return $value;
    }

    public function getItem(mixed $key): CacheItem
    {
        $item = new CacheItem();
        $reflection = new ReflectionClass($item);

        $keyProperty = $reflection->getProperty('key');
        $keyProperty->setValue($item, $key);

        if (isset($this->cache[$key])) {
            $isHitProperty = $reflection->getProperty('isHit');
            $isHitProperty->setValue($item, true);

            $valueProperty = $reflection->getProperty('value');
            $valueProperty->setValue($item, $this->cache[$key]);
        }

        return $item;
    }

    /**
     * @param array<string> $keys
     *
     * @return iterable<string, CacheItem>
     */
    public function getItems(array $keys = []): iterable
    {
        foreach ($keys as $key) {
            yield $key => $this->getItem($key);
        }
    }

    public function hasItem(mixed $key): bool
    {
        return isset($this->cache[$key]);
    }

    /**
     * @param array<string> $tags
     */
    public function invalidateTags(array $tags): bool
    {
        $tagsSet = array_flip($tags);

        foreach ($this->tags as $key => $itemTags) {
            foreach ($itemTags as $tag) {
                if (isset($tagsSet[$tag])) {
                    unset($this->cache[$key], $this->tags[$key]);

                    break;
                }
            }
        }

        return true;
    }

    public function prune(): bool
    {
        return true;
    }

    public function reset(): void
    {
        $this->cache = [];
        $this->tags = [];
        $this->deferred = [];
    }

    public function save(CacheItemInterface $item): bool
    {
        $reflection = new ReflectionClass($item);
        $valueProperty = $reflection->getProperty('value');
        $this->cache[$item->getKey()] = $valueProperty->getValue($item);

        return true;
    }

    public function saveDeferred(CacheItemInterface $item): bool
    {
        if ($item instanceof CacheItem) {
            $this->deferred[$item->getKey()] = $item;
        }

        return true;
    }
}
