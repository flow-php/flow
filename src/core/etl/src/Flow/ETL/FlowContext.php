<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;
use Flow\Serializer\Serializer;

final class FlowContext
{
    private SaveMode $mode = SaveMode::ExceptionIfExists;

    private PartitionFilter $partitionFilter;

    /**
     * @var array<string>
     */
    private array $partitions;

    public function __construct(public readonly Config $config)
    {
        $this->partitionFilter = new NoopFilter();
        $this->partitions = [];
    }

    public function cache() : Cache
    {
        return $this->config->cache();
    }

    public function filterPartitions(PartitionFilter $filter) : self
    {
        $this->partitionFilter = $filter;

        return $this;
    }

    public function fs() : Filesystem
    {
        return $this->config->filesystem();
    }

    public function mode() : SaveMode
    {
        return $this->mode;
    }

    public function partitionBy(string ...$entry) : self
    {
        $this->partitions = $entry;

        return $this;
    }

    /**
     * @return array<string>
     */
    public function partitionEntries() : array
    {
        return $this->partitions;
    }

    public function partitionFilter() : PartitionFilter
    {
        return $this->partitionFilter;
    }

    public function serializer() : Serializer
    {
        return $this->config->serializer();
    }

    public function setMode(SaveMode $mode) : self
    {
        $this->mode = $mode;

        return $this;
    }
}
