<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface RowsFactory extends Serializable
{
    /**
     * @param array<array<mixed>> $data
     */
    public function create(array $data) : Rows;
}
