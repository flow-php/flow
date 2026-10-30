<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime;

use Flow\ETL\Storage\RowsStorage;
use Flow\Filesystem\Path;

final readonly class CacheConfig
{
    public const string CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public function __construct(
        public RowsStorage $cache,
        public Path $localFilesystemCacheDir,
    ) {}
}
