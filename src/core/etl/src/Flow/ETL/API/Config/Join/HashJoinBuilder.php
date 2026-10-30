<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config\Join;

use Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Planning\Logical\Node\JoinAlgorithmBuilder;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path;

final class HashJoinBuilder implements JoinAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    private ?Unit $broadcastLimit = null;

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

    /**
     * The right side's size under which a join read by workers broadcasts it to every task; default 10 MB.
     */
    public function broadcastLimit(Unit $broadcastLimit): self
    {
        if ($broadcastLimit->inBytes() < 1) {
            throw new InvalidArgumentException('Join broadcast limit must be greater than 0 bytes');
        }

        $this->broadcastLimit = $broadcastLimit;

        return $this;
    }

    public function build(Path $spillRoot, Backend $backend): HashJoinConfig
    {
        return new HashJoinConfig(
            $this->bucketing->build($spillRoot, $backend),
            $this->broadcastLimit ?? Unit::fromMb(10),
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
        $this->bucketing->memoryLimit($memoryLimit);

        return $this;
    }

    public function storage(RowsStorage $storage): self
    {
        $this->bucketing->storage($storage);

        return $this;
    }
}
