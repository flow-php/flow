<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowsCache;

use Flow\ETL\{Cache\RowsCache, Rows};

final class NullCache implements RowsCache
{
    public function add(string $id, Rows $rows) : void
    {
    }

    public function clear(string $id) : void
    {
    }

    public function has(string $id) : bool
    {
        return false;
    }

    /**
     * @psalm-suppress InvalidReturnType
     */
    public function read(string $id) : \Generator
    {
        yield from [];
    }
}
