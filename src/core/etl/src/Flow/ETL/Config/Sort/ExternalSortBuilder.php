<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Column\Backend;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Config\MemoryLimit;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;

final class ExternalSortBuilder implements SortAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    private ?BucketsStorage $mergeStorage = null;

    private ?Unit $memoryLimit = null;

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

    public function build(Path $spillRoot, Backend $backend): ExternalSortConfig
    {
        return new ExternalSortConfig(
            $this->bucketing->build($spillRoot, $backend),
            $this->memoryLimit ?? MemoryLimit::default(),
            $this->mergeStorage,
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
     * Storage for merged runs only. Defaults to the spill storage, so storage() keeps covering both phases.
     */
    public function mergeStorage(BucketsStorage $storage): self
    {
        $this->mergeStorage = $storage;

        return $this;
    }

    /**
     * The process memory past which the sort spills sorted runs; defaults to MemoryLimit::default().
     */
    public function memoryLimit(Unit $memoryLimit): self
    {
        if ($memoryLimit->inBytes() < 1) {
            throw new InvalidArgumentException('Sort memory limit must be greater than 0 bytes');
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
