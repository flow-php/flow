<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use APCUIterator;
use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Generator;

use function apcu_delete;
use function apcu_enabled;
use function apcu_exists;
use function apcu_fetch;
use function apcu_store;
use function extension_loaded;
use function preg_quote;
use function sprintf;

final readonly class ApcuCache implements Cache
{
    public function __construct(
        private string $namespace = 'flow_php_cache',
    ) {
        if (!extension_loaded('apcu') || !apcu_enabled()) {
            throw new RuntimeException(
                'ApcuCache requires the APCu extension (ext-apcu) with apc.enabled=1'
                . (PHP_SAPI === 'cli' ? ' and apc.enable_cli=1' : ''),
            );
        }
    }

    public function clear(): void
    {
        $iterator = new APCUIterator('/^' . preg_quote($this->namespace . ':', '/') . '/');

        apcu_delete($iterator);
    }

    public function delete(string $key): void
    {
        apcu_delete($this->namespacedKey($key));
    }

    public function get(string $key): Rows
    {
        $success = false;

        // @mago-ignore analysis:mixed-assignment
        $value = apcu_fetch($this->namespacedKey($key), $success);

        if (!$success) {
            throw new KeyNotInCacheException($key);
        }

        if (!$value instanceof Rows) {
            throw new RuntimeException(sprintf(
                'Cache entry for key "%s" is corrupted or was not written by ApcuCache.',
                $key,
            ));
        }

        return $value;
    }

    public function has(string $key): bool
    {
        return (bool) apcu_exists($this->namespacedKey($key));
    }

    /**
     * @throws KeyNotInCacheException
     *
     * @return Generator<int, Rows>
     */
    public function read(string $key): Generator
    {
        yield $this->get($key);
    }

    public function set(string $key, Rows $value): void
    {
        $result = apcu_store($this->namespacedKey($key), $value);

        if ($result === false) {
            throw new RuntimeException(sprintf(
                'Failed to store cache entry for key "%s" in APCu, the cache segment might be full.',
                $key,
            ));
        }
    }

    private function namespacedKey(string $key): string
    {
        return $this->namespace . ':' . $key;
    }
}
