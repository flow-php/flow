<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\Serializer\Serializer;

/**
 * Mutable Flow execution context.
 * It can be modified through the DataFrame.
 */
final class FlowContext
{
    private ErrorHandler $errorHandler;

    private SaveMode $mode = SaveMode::ExceptionIfExists;

    private PartitionFilter $partitionFilter;

    private References $partitions;

    private bool $threadSafe = false;

    public function __construct(public readonly Config $config)
    {
        $this->partitionFilter = new NoopFilter();
        $this->errorHandler = new ThrowError();
        $this->partitions = new References();
    }

    public function cache() : Cache
    {
        return $this->config->cache();
    }

    public function entryFactory() : EntryFactory
    {
        return $this->config->entryFactory();
    }

    public function errorHandler() : ErrorHandler
    {
        return $this->errorHandler;
    }

    public function filterPartitions(PartitionFilter $filter) : self
    {
        $this->partitionFilter = $filter;

        return $this;
    }

    public function mode() : SaveMode
    {
        return $this->mode;
    }

    public function partitionBy(string|Reference ...$entry) : self
    {
        $this->partitions = References::init(...$entry);

        return $this;
    }

    public function partitionEntries() : References
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

    public function setErrorHandler(ErrorHandler $handler) : self
    {
        $this->errorHandler = $handler;

        return $this;
    }

    public function setMode(SaveMode $mode) : self
    {
        $this->mode = $mode;

        return $this;
    }

    public function setThreadSafe(bool $threadSafe = true) : self
    {
        $this->threadSafe = $threadSafe;

        return $this;
    }

    public function streams() : FilesystemStreams
    {
        return $this->config->filesystemStreams();
    }

    public function threadSafe() : bool
    {
        return $this->threadSafe;
    }
}
