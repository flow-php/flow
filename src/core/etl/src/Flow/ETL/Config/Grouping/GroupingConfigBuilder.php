<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class GroupingConfigBuilder
{
    /**
     * @var int<1, max>
     */
    private int $batchSize = 1000;

    /**
     * @var int<1, max>
     */
    private int $bucketsCount = 64;

    private string $filesystemMount = 'file';

    private ?BucketsStorage $storage = null;

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

    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): GroupingConfig
    {
        $storage = $this->storage ?? new FilesystemBuckets(
            $filesystemTable->for($this->filesystemMount),
            $localFilesystemCacheDir->suffix('/flow-php-group-by/'),
            $this->batchSize,
        );

        return new GroupingConfig($storage, $this->bucketsCount, $this->batchSize);
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

    public function storage(BucketsStorage $storage): self
    {
        $this->storage = $storage;

        return $this;
    }
}
