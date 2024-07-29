<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Cache\{Implementation\FilesystemCache};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\{FilesystemTable, Path};
use Flow\Serializer\Serializer;

final class CacheConfigBuilder
{
    private ?\Flow\ETL\Cache $cache = null;

    /**
     * @var int<1, max>
     */
    private int $cacheBatchSize = 100;

    /**
     * @var int<1, max>
     */
    private int $externalSortBucketsCount = 100;

    public function build(FilesystemTable $fstab, Serializer $serializer) : CacheConfig
    {
        $cachePath = \is_string(\getenv(CacheConfig::CACHE_DIR_ENV)) && \getenv(CacheConfig::CACHE_DIR_ENV) !== ''
            ? \getenv(CacheConfig::CACHE_DIR_ENV)
            : \sys_get_temp_dir() . '/flow_php/cache';

        if (!\is_string($cachePath)) {
            throw new RuntimeException('Cache directory must be a string, got ' . \gettype($cachePath));
        }

        if (!\file_exists($cachePath)) {
            if (!mkdir($cachePath, 0777, true) && !is_dir($cachePath)) {
                throw new RuntimeException(sprintf('Can\'t create cache directory: "%s" Please use a different one through %s environment variable', $cachePath, CacheConfig::CACHE_DIR_ENV));
            }
        }

        return new CacheConfig(
            cache: $this->cache ?? new FilesystemCache(
                $fstab->for(protocol('file')),
                $serializer,
                cacheDir: Path::realpath($cachePath)
            ),
            cacheBatchSize: $this->cacheBatchSize,
            localFilesystemCacheDir: Path::realpath($cachePath),
            externalSortBucketsCount: $this->externalSortBucketsCount
        );
    }

    public function cache(\Flow\ETL\Cache $cache) : self
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * @param int<1, max> $cacheBatchSize
     */
    public function cacheBatchSize(int $cacheBatchSize) : self
    {
        if ($cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        $this->cacheBatchSize = $cacheBatchSize;

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
