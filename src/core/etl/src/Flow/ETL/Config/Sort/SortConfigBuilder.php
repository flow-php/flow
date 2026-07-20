<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class SortConfigBuilder
{
    private SortAlgorithms $algorithm = SortAlgorithms::EXTERNAL_SORT;

    /**
     * @var int<1, max>
     */
    private int $batchSize = 1000;

    /**
     * @var int<1, max>
     */
    private int $bucketSize = 10_000;

    /**
     * @var int<1, max>
     */
    private int $bucketsCount = 100;

    private ?BucketsStorage $cache = null;

    private string $filesystemProtocol = 'file';

    public function algorithm(SortAlgorithms $algorithm): self
    {
        $this->algorithm = $algorithm;

        return $this;
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function batchSize(int $batchSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        $this->batchSize = $batchSize;

        return $this;
    }

    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): SortConfig
    {
        $cache = $this->cache ?? new FilesystemBuckets(
            $filesystemTable->for($this->filesystemProtocol),
            $localFilesystemCacheDir->suffix('/flow-php-sort/'),
            $this->batchSize,
        );

        return new SortConfig(
            $this->algorithm,
            $cache,
            $this->bucketsCount,
            $this->bucketSize,
            $this->batchSize,
            $this->filesystemProtocol,
        );
    }

    /**
     * @param int<1, max> $bucketSize
     */
    public function bucketSize(int $bucketSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($bucketSize < 1) {
            throw new InvalidArgumentException('Bucket size must be greater than 0');
        }

        $this->bucketSize = $bucketSize;

        return $this;
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public function bucketsCount(int $bucketsCount): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0');
        }

        $this->bucketsCount = $bucketsCount;

        return $this;
    }

    public function cache(BucketsStorage $cache): self
    {
        $this->cache = $cache;

        return $this;
    }

    public function filesystemProtocol(string $protocol): self
    {
        $this->filesystemProtocol = $protocol;

        return $this;
    }
}
