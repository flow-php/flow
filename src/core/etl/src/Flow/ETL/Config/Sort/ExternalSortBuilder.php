<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class ExternalSortBuilder implements SortAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    /**
     * @var int<1, max>
     */
    private int $runSize = 10_000;

    public function __construct()
    {
        $this->bucketing = new BucketingConfigBuilder('/flow-php-sort/', 100);
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function batchSize(int $batchSize): self
    {
        $this->bucketing->batchSize($batchSize);

        return $this;
    }

    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): ExternalSortConfig
    {
        return new ExternalSortConfig(
            $this->bucketing->build($filesystemTable, $localFilesystemCacheDir),
            $this->runSize,
        );
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public function bucketsCount(int $bucketsCount): self
    {
        $this->bucketing->bucketsCount($bucketsCount);

        return $this;
    }

    public function filesystemProtocol(string $protocol): self
    {
        $this->bucketing->filesystemProtocol($protocol);

        return $this;
    }

    /**
     * @param int<1, max> $runSize
     */
    public function runSize(int $runSize): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($runSize < 1) {
            throw new InvalidArgumentException('Run size must be greater than 0');
        }

        $this->runSize = $runSize;

        return $this;
    }

    public function storage(BucketsStorage $storage): self
    {
        $this->bucketing->storage($storage);

        return $this;
    }
}
