<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Cache;
use Flow\ETL\Cache\{Implementation\FilesystemCache};
use Flow\ETL\Exception\{InvalidArgumentException};
use Flow\Filesystem\{FilesystemTable, Path};
use Flow\Serializer\Serializer;

final class CacheConfigBuilder
{
    private ?Cache $cache = null;

    /**
     * @var int<1, max>
     */
    private int $externalSortBucketsCount = 100;

    public function build(FilesystemTable $fstab, Serializer $serializer) : CacheConfig
    {
        $cachePath = \getenv(CacheConfig::CACHE_DIR_ENV) ?: '';
        $cachePath = Path::realpath($cachePath !== '' ? $cachePath : \sys_get_temp_dir() . '/flow_php/cache');

        return new CacheConfig(
            cache: $this->cache ?? new FilesystemCache(
                $fstab->for(protocol('file')),
                $serializer,
                cacheDir: $cachePath
            ),
            localFilesystemCacheDir: $cachePath,
            externalSortBucketsCount: $this->externalSortBucketsCount
        );
    }

    public function cache(Cache $cache) : self
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBucketsCount
     */
    public function externalSortBucketsCount(int $externalSortBucketsCount) : self
    {
        if ($externalSortBucketsCount < 1) {
            throw new InvalidArgumentException('External sort buckets count must be greater than 0');
        }

        $this->externalSortBucketsCount = $externalSortBucketsCount;

        return $this;
    }
}
