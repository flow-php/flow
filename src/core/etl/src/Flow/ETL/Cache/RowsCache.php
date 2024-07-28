<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Rows;

interface RowsCache
{
    public function add(string $id, Rows $rows) : void;

    public function clear(string $id) : void;

    public function has(string $id) : bool;

    /**
     * @return \Generator<Rows>
     */
    public function read(string $id) : \Generator;
}
