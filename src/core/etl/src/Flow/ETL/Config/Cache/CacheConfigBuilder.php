<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Cache\Implementation\TraceableCache;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\FilesystemTable;
use Flow\Serializer\Serializer;

use function Flow\Filesystem\DSL\path_real;
use function getenv;
use function sys_get_temp_dir;

final class CacheConfigBuilder
{
    private ?Cache $cache = null;

    /**
     * @var int<1, max>
     */
    private int $externalSortBucketsCount = 100;

    /**
     * @var int<1, max>
     */
    private int $externalSortBatchSize = 1000;

    /**
     * @var int<1, max>
     */
    private int $externalSortBucketSize = 10_000;

    private string $filesystemMount = 'file';

    public function build(
        FilesystemTable $fstab,
        Serializer $serializer,
        ?TelemetryConfig $telemetryConfig = null,
        string $dataframeName = 'flow_dataframe',
    ): CacheConfig {
        $cachePath = getenv(CacheConfig::CACHE_DIR_ENV) ?: '';
        $cachePath = path_real($cachePath !== '' ? $cachePath : sys_get_temp_dir() . '/flow_php/cache');

        $cache = $this->cache ?? new FilesystemCache(
            $fstab->for($this->filesystemMount),
            cacheDir: $cachePath,
            serializer: $serializer,
        );

        if ($telemetryConfig !== null && $telemetryConfig->options->traceCache) {
            $cache = new TraceableCache($cache, $telemetryConfig->telemetry, $dataframeName);
        }

        return new CacheConfig(
            cache: $cache,
            localFilesystemCacheDir: $cachePath,
            externalSortBucketsCount: $this->externalSortBucketsCount,
            externalSortBatchSize: $this->externalSortBatchSize,
            externalSortBucketSize: $this->externalSortBucketSize,
            filesystemMount: $this->filesystemMount,
        );
    }

    public function cache(Cache $cache): self
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * @param int<1, max> $externalSortBucketsCount
     */
    public function externalSortBucketsCount(int $externalSortBucketsCount): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($externalSortBucketsCount < 1) {
            throw new InvalidArgumentException('External sort buckets count must be greater than 0');
        }

        $this->externalSortBucketsCount = $externalSortBucketsCount;

        return $this;
    }

    /**
     * Rows per Floe crossing when the external sort spills and reads its buckets.
     *
     * @param int<1, max> $externalSortBatchSize
     */
    public function externalSortBatchSize(int $externalSortBatchSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($externalSortBatchSize < 1) {
            throw new InvalidArgumentException('External sort batch size must be at least 1');
        }

        $this->externalSortBatchSize = $externalSortBatchSize;

        return $this;
    }

    /**
     * Rows buffered and sorted in memory before the external sort spills them as one bucket.
     *
     * @param int<1, max> $externalSortBucketSize
     */
    public function externalSortBucketSize(int $externalSortBucketSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($externalSortBucketSize < 1) {
            throw new InvalidArgumentException('External sort bucket size must be at least 1');
        }

        $this->externalSortBucketSize = $externalSortBucketSize;

        return $this;
    }

    public function filesystemMount(string $mount): self
    {
        $this->filesystemMount = $mount;

        return $this;
    }
}
