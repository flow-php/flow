<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Rows;

/**
 * Cache whole Rows object at once with possibility to append more rows to the same key.
 */
interface RowsCache
{
    public function append(string $key, Rows $rows) : void;

    /**
     * @return \Generator<Rows>
     */
    public function get(string $key) : \Generator;

    public function has(string $key) : bool;

    public function remove(string $key) : void;
}
