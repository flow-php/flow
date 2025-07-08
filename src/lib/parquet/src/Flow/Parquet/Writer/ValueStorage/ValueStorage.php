<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ValueStorage;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface ValueStorage
{
    /**
     * @param array<mixed> $values
     */
    public function addValues(FlatColumn $column, array $values) : void;

    public function getBuffer() : string;

    public function isEmpty() : bool;

    public function reset() : void;

    public function size() : int;
}
