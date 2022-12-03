<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Rows;

/**
 * @implements Cache<array<mixed>>
 */
final class NullCache implements Cache
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function add(string $id, Rows $rows) : void
    {
    }

    public function clear(string $id) : void
    {
    }

    /**
     * @psalm-suppress InvalidReturnType
     */
    public function read(string $id) : \Generator
    {
        yield from [];
    }
}
