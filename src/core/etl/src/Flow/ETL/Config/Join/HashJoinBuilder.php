<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class HashJoinBuilder implements JoinAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    public function __construct()
    {
        $this->bucketing = new BucketingConfigBuilder('/flow-php-join/', 64);
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function batchSize(int $batchSize): self
    {
        $this->bucketing->batchSize($batchSize);

        return $this;
    }

    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): HashJoinConfig
    {
        return new HashJoinConfig($this->bucketing->build($filesystemTable, $localFilesystemCacheDir));
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

    public function storage(BucketsStorage $storage): self
    {
        $this->bucketing->storage($storage);

        return $this;
    }
}
