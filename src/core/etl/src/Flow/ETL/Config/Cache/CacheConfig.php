<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use Flow\ETL\Cache;
use Flow\Filesystem\Path;

final readonly class CacheConfig
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    /**
     * @param int<1, max> $externalSortBucketsCount
     */
    public function __construct(
        public Cache $cache,
        public Path $localFilesystemCacheDir,
        public int $externalSortBucketsCount,
    ) {
    }
}
