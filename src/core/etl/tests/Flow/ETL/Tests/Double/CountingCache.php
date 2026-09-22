<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Cache;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

/**
 * Counts get() calls so a test can assert how many times a cached entry is loaded.
 */
final class CountingCache implements Cache
{
    public int $getCalls = 0;

    public function __construct(
        private readonly Cache $inner,
    ) {}

    public function clear(): void
    {
        $this->inner->clear();
    }

    public function delete(string $key): void
    {
        $this->inner->delete($key);
    }

    public function get(string $key): Rows
    {
        $this->getCalls++;

        return $this->inner->get($key);
    }

    public function has(string $key): bool
    {
        return $this->inner->has($key);
    }

    public function schema(string $key): Schema
    {
        return $this->inner->schema($key);
    }

    public function set(string $key, Rows $value): void
    {
        $this->inner->set($key, $value);
    }
}
