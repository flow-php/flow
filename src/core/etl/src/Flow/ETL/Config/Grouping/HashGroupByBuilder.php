<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\Filesystem\Path;

final class HashGroupByBuilder implements GroupByAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    public function __construct()
    {
        $this->bucketing = new BucketingConfigBuilder('/flow-php-group-by/', 64);
    }

    /**
     * @param int<1, max> $batchSize
     */
    public function batchSize(int $batchSize): self
    {
        $this->bucketing->batchSize($batchSize);

        return $this;
    }

    public function build(Path $spillRoot): HashGroupByConfig
    {
        return new HashGroupByConfig($this->bucketing->build($spillRoot));
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public function bucketsCount(int $bucketsCount): self
    {
        $this->bucketing->bucketsCount($bucketsCount);

        return $this;
    }

    public function storage(BucketsStorage $storage): self
    {
        $this->bucketing->storage($storage);

        return $this;
    }
}
