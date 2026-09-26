<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self;

    public function fromParquetType(mixed $data): mixed;

    public function toParquetType(mixed $data): mixed;
}
