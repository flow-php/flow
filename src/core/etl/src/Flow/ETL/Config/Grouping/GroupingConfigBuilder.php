<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class GroupingConfigBuilder
{
    /**
     * @var int<1, max>
     */
    private int $batchSize = 1000;

    private ?BucketsCache $cache = null;

    private string $filesystemMount = 'file';

    /**
     * @var int<1, max>
     */
    private int $partitions = 64;

    private bool $spillToFilesystem = false;

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

    public function build(FilesystemTable $fstab, Path $localFilesystemCacheDir): GroupingConfig
    {
        $cache = $this->cache;

        if ($cache === null && $this->spillToFilesystem) {
            $cache = new FilesystemBucketsCache(
                $fstab->for($this->filesystemMount),
                $localFilesystemCacheDir->suffix('/flow-php-group-by/'),
                $this->batchSize,
            );
        }

        return new GroupingConfig($cache, $this->partitions, $this->batchSize);
    }

    public function filesystem(?BucketsCache $cache = null): self
    {
        $this->spillToFilesystem = true;
        $this->cache = $cache;

        return $this;
    }

    public function filesystemMount(string $mount): self
    {
        $this->filesystemMount = $mount;

        return $this;
    }

    /**
     * @param int<1, max> $partitions
     */
    public function partitions(int $partitions): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($partitions < 1) {
            throw new InvalidArgumentException('Partitions count must be greater than 0');
        }

        $this->partitions = $partitions;

        return $this;
    }
}
