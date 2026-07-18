<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class JoinConfigBuilder
{
    /**
     * @var int<1, max>
     */
    private int $batchSize = 1000;

    /**
     * @var int<1, max>
     */
    private int $bucketsCount = 64;

    private ?BucketsCache $cache = null;

    private string $filesystemMount = 'file';

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

    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): JoinConfig
    {
        $cache = $this->cache ?? new FilesystemBucketsCache(
            $filesystemTable->for($this->filesystemMount),
            $localFilesystemCacheDir->suffix('/flow-php-join/'),
            $this->batchSize,
        );

        return new JoinConfig($cache, $this->bucketsCount, $this->batchSize);
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

    public function cache(BucketsCache $cache): self
    {
        $this->cache = $cache;

        return $this;
    }
}
