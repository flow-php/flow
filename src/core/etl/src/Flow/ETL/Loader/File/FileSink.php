<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\File;

use Flow\ETL\Rows;

interface FileSink
{
    public function close(): void;

    public function write(Rows $rows): void;
}
