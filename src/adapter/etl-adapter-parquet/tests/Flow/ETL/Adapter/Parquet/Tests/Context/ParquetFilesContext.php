<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\Filesystem\DSL\path;

final class ParquetFilesContext
{
    /**
     * @param array<string, Rows> $files - uri => the rows written to it
     */
    public static function write(Filesystem $filesystem, array $files): void
    {
        foreach ($files as $uri => $rows) {
            data_frame()
                ->read(from_rows($rows))
                ->write(to_parquet(path($uri), filesystem: $filesystem))
                ->run();
        }
    }
}
