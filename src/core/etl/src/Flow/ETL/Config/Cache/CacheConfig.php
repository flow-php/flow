<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use Flow\ETL\Cache\{RowCache, RowsCache};
use Flow\Filesystem\Path;

final class CacheConfig
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    /**
     * @param RowsCache $rowsCache
     * @param RowCache $rowCache
     * @param int<1, max> $cacheBatchSize
     */
    public function __construct(
        public readonly RowsCache $rowsCache,
        public readonly RowCache $rowCache,
        public readonly int $cacheBatchSize,
        public readonly Path $localFilesystemCacheDir,
    ) {
    }
}
