<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowsCache;

use Flow\ETL\{Cache\RowsCache, Rows};

final class NullCache implements RowsCache
{
    public function append(string $key, Rows $rows) : void
    {
    }

    /**
     * @psalm-suppress InvalidReturnType
     */
    public function get(string $key) : \Generator
    {
        yield from [];
    }

    public function has(string $key) : bool
    {
        return false;
    }

    public function remove(string $key) : void
    {
    }
}
