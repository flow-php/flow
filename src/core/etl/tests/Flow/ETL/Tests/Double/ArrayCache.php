<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateInterval;
use Psr\SimpleCache\CacheInterface;

use function array_key_exists;

/**
 * Parameters are intentionally untyped: psr/simple-cache 1.0 declares CacheInterface without
 * parameter types, so narrowing them here is an LSP violation that PHP reports as a fatal error
 * whenever the lowest supported version is installed. Untyped parameters stay compatible with
 * 1.0, 2.0 and 3.0 alike.
 */
final class ArrayCache implements CacheInterface
{
    /**
     * @var array<string, mixed>
     */
    private array $values = [];

    public function clear(): bool
    {
        $this->values = [];

        return true;
    }

    /**
     * @param string $key
     */
    public function delete($key): bool
    {
        unset($this->values[$key]);

        return true;
    }

    /**
     * @param iterable<string> $keys
     */
    public function deleteMultiple($keys): bool
    {
        foreach ($keys as $key) {
            $this->delete($key);
        }

        return true;
    }

    /**
     * @param string $key
     */
    public function get($key, mixed $default = null): mixed
    {
        return array_key_exists($key, $this->values) ? $this->values[$key] : $default;
    }

    /**
     * @param iterable<string> $keys
     *
     * @return iterable<string, mixed>
     */
    public function getMultiple($keys, mixed $default = null): iterable
    {
        $result = [];

        foreach ($keys as $key) {
            $result[$key] = $this->get($key, $default);
        }

        return $result;
    }

    /**
     * @param string $key
     */
    public function has($key): bool
    {
        return array_key_exists($key, $this->values);
    }

    /**
     * @param string $key
     * @param null|DateInterval|int $ttl
     */
    public function set($key, mixed $value, $ttl = null): bool
    {
        $this->values[$key] = $value;

        return true;
    }

    /**
     * @param iterable $values
     * @param null|DateInterval|int $ttl
     */
    public function setMultiple($values, $ttl = null): bool
    {
        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $key => $value) {
            $this->set((string) $key, $value, $ttl);
        }

        return true;
    }
}
