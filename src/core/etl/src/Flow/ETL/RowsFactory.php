<?php declare(strict_types=1);

namespace Flow\ETL;

interface RowsFactory
{
    /**
     * @param array<array<mixed>> $data
     */
    public function create(array $data) : Rows;
}
