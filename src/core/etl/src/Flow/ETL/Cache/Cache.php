<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\{Row, Rows};

interface Cache
{
    public function clear() : void;

    public function delete(string $key) : void;

    /**
     * @throws KeyNotInCacheException
     */
    public function get(string $key) : Row|Rows|CacheIndex;

    public function has(string $key) : bool;

    public function set(string $key, Row|Rows|CacheIndex $value) : void;
}
