<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\{Row, Rows};

interface SortRowCache
{
    /**
     * @return \Generator<Row>
     */
    public function get(string $key) : \Generator;

    public function remove(string $key) : void;

    public function set(string $key, Rows $rows) : void;
}
