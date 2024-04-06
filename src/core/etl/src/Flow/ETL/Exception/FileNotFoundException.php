<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Filesystem\Path;

final class FileNotFoundException extends Exception
{
    public function __construct(public readonly Path $path, ?\Throwable $previous = null)
    {
        parent::__construct(
            \sprintf('No such file or directory: %s ', $this->path->uri()),
            0,
            $previous
        );
    }
}
