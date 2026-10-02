<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

interface ParquetOpenSink
{
    public function close(): void;

    public function write(Rows $rows): void;
}
