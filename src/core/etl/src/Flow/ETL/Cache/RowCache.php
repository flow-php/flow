<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\{Row, Rows};

interface RowCache
{
    /**
     * @return \Generator<Row>
     */
    public function get(string $key) : \Generator;

    public function remove(string $key) : void;

    /**
     * @param iterable<Row>|Rows $rows
     */
    public function set(string $key, iterable|Rows $rows) : void;
}
