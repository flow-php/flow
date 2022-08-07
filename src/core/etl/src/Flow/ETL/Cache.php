<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface Cache extends Serializable
{
    public function add(string $id, Rows $rows) : void;

    public function clear(string $id) : void;

    /**
     * @return \Generator<Rows>
     */
    public function read(string $id) : \Generator;
}
