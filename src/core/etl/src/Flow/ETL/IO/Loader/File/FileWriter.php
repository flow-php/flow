<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Values\Rows;

use function interface_exists;

if (interface_exists(FileWriter::class, false)) {
    return;
}

interface FileWriter
{
    public function close(): void;

    public function write(Rows $rows): void;
}
