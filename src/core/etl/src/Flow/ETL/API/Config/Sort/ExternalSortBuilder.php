<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config\Sort;

use Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path;

final class ExternalSortBuilder implements SortAlgorithmBuilder
{
    private readonly BucketingConfigBuilder $bucketing;

    private ?RowsStorage $mergeStorage = null;

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
        return new ExternalSortConfig($this->bucketing->build($spillRoot, $backend), $this->mergeStorage);
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
    public function mergeStorage(RowsStorage $storage): self
    {
        $this->mergeStorage = $storage;

        return $this;
    }

    /**
     * The process memory past which the sort spills sorted runs; defaults to MemoryLimit::default().
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
