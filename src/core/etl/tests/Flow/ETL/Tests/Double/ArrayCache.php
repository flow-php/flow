<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateInterval;
use Psr\SimpleCache\CacheInterface;

use function array_key_exists;

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

    public function delete(string $key): bool
    {
        unset($this->values[$key]);

        return true;
    }

    public function deleteMultiple(iterable $keys): bool
    {
        foreach ($keys as $key) {
            $this->delete($key);
        }

        return true;
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return array_key_exists($key, $this->values) ? $this->values[$key] : $default;
    }

    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $result = [];

        foreach ($keys as $key) {
            $result[$key] = $this->get($key, $default);
        }

        return $result;
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->values);
    }

    public function set(string $key, mixed $value, int|DateInterval|null $ttl = null): bool
    {
        $this->values[$key] = $value;

        return true;
    }

    public function setMultiple(iterable $values, int|DateInterval|null $ttl = null): bool
    {
        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $key => $value) {
            $this->set((string) $key, $value, $ttl);
        }

        return true;
    }
}
