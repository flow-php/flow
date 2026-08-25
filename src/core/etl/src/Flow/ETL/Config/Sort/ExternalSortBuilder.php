<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;

final class ExternalSortBuilder implements SortAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    private ?BucketsStorage $mergeStorage = null;

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

    public function build(Path $spillRoot): ExternalSortConfig
    {
        return new ExternalSortConfig($this->bucketing->build($spillRoot), $this->mergeStorage, $this->runSize);
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
     * Storage for merged runs only. Defaults to the spill storage, so storage() keeps covering both phases.
     */
    public function mergeStorage(BucketsStorage $storage): self
    {
        $this->mergeStorage = $storage;

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
