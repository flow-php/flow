<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Rows;

final class NullCache implements Cache
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
