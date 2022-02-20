<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializable;

interface RowsFactory extends Serializable
{
    /**
     * @param array<array<mixed>> $data
     *
     * @return Rows
     */
    public function create(array $data) : Rows;
}
