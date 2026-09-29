<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Column\Backend;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Config\MemoryLimit;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;

final class HashJoinBuilder implements JoinAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    private ?Unit $memoryLimit = null;

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

    public function build(Path $spillRoot, Backend $backend): HashJoinConfig
    {
        return new HashJoinConfig(
            $this->bucketing->build($spillRoot, $backend),
            $this->memoryLimit ?? MemoryLimit::default(),
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

    /**
     * The process memory past which the build side and the buckets move to the storage; defaults to
     * MemoryLimit::default().
     */
    public function memoryLimit(Unit $memoryLimit): self
    {
        if ($memoryLimit->inBytes() < 1) {
            throw new InvalidArgumentException('Join memory limit must be greater than 0 bytes');
        }

        $this->memoryLimit = $memoryLimit;

        return $this;
    }

    public function storage(BucketsStorage $storage): self
    {
        $this->bucketing->storage($storage);

        return $this;
    }
}
