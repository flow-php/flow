<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Cache\{RowCache, RowsCache};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\{FilesystemTable, Path};
use Flow\Serializer\Serializer;

final class CacheConfigBuilder
{
    /**
     * @var int<1, max>
     */
    private int $cacheBatchSize = 1000;

    private ?RowCache $rowCache = null;

    private ?RowsCache $rowsCache = null;

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

        if ($this->rowsCache === null) {
            $this->rowsCache = new RowsCache\FilesystemCache(
                $fstab->for(protocol('file')),
                $serializer,
                Path::realpath($cachePath)
            );
        }

        if ($this->rowCache === null) {
            $this->rowCache = new RowCache\FilesystemCache(
                $fstab->for(protocol('file')),
                $serializer,
                chunkSize: 100,
                cacheDir: Path::realpath($cachePath)
            );
        }

        return new CacheConfig(
            rowsCache: $this->rowsCache,
            rowCache: $this->rowCache,
            cacheBatchSize: $this->cacheBatchSize,
            localFilesystemCacheDir: Path::realpath($cachePath)
        );
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

    public function rowCache(RowCache $rowCache) : self
    {
        $this->rowCache = $rowCache;

        return $this;
    }

    public function rowsCache(RowsCache $rowsCache) : self
    {
        $this->rowsCache = $rowsCache;

        return $this;
    }
}
