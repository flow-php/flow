<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use Flow\ETL\Cache;
use Flow\Filesystem\Path;

final readonly class CacheConfig
{
    public const string CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    /**
     * @param int<1, max> $externalSortBucketsCount
     * @param int<1, max> $externalSortBatchSize
     * @param int<1, max> $externalSortBucketSize
     */
    public function __construct(
        public Cache $cache,
        public Path $localFilesystemCacheDir,
        public int $externalSortBucketsCount,
        public int $externalSortBatchSize = 1000,
        public int $externalSortBucketSize = 10_000,
        public string $filesystemMount = 'file',
    ) {}
}
