<?php declare(strict_types=1);

namespace Flow\ETL;

interface Cache
{
    public function add(string $id, Rows $rows) : void;

    /**
     * @param string $id
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function read(string $id) : \Generator;

    public function clear(string $id) : void;
}
