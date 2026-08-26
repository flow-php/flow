<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use APCUIterator;
use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

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
        apcu_delete([$this->namespacedKey($key), $this->schemaKey($key)]);
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

    public function schema(string $key): Schema
    {
        $success = false;

        // @mago-ignore analysis:mixed-assignment
        $value = apcu_fetch($this->schemaKey($key), $success);

        if (!$success) {
            throw new KeyNotInCacheException($key);
        }

        if (!$value instanceof Schema) {
            throw new RuntimeException(sprintf(
                'Cached schema for key "%s" is corrupted or was not written by ApcuCache.',
                $key,
            ));
        }

        return $value;
    }

    public function set(string $key, Rows $value): void
    {
        if (
            apcu_store($this->namespacedKey($key), $value) === false
            || apcu_store($this->schemaKey($key), $value->schema()) === false
        ) {
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

    private function schemaKey(string $key): string
    {
        return $this->namespacedKey($key) . ':schema';
    }
}
