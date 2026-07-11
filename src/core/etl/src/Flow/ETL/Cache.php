<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\KeyNotInCacheException;
use Generator;

interface Cache
{
    public function clear(): void;

    public function delete(string $key): void;

    /**
     * @throws KeyNotInCacheException
     */
    public function get(string $key): Rows;

    /**
     * @throws KeyNotInCacheException during iteration when the key is absent
     *
     * @return Generator<int, Rows>
     */
    public function read(string $key): Generator;

    public function has(string $key): bool;

    public function set(string $key, Rows $value): void;
}
