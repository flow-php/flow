<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;

use function interface_exists;

if (interface_exists(ParquetOpenSink::class, false)) {
    return;
}

interface ParquetOpenSink
{
    public function close(): void;

    public function write(Rows $rows): void;
}
