<?php declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface Converter
{
    public function fromParquetType(mixed $data) : mixed;

    public function isFor(FlatColumn $column, Options $options) : bool;

    public function toParquetType(mixed $data) : mixed;
}
