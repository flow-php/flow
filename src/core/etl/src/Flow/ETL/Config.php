<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\EntryFactory;

/**
 * Immutable configuration that can be used to initialize many contexts.
 * Configuration must not be changed after it's passed to FlowContext.
 */
final class Config
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public const EXTERNAL_SORT_MAX_MEMORY_ENV = 'FLOW_EXTERNAL_SORT_MAX_MEMORY';

    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly ExternalSort $externalSort,
        private readonly FilesystemStreams $filesystemStreams,
        private readonly Optimizer $optimizer,
        private readonly bool $putInputIntoRows,
        private readonly EntryFactory $entryFactory
    ) {
    }

    public static function builder() : ConfigBuilder
    {
        return new ConfigBuilder();
    }

    public static function default() : self
    {
        return self::builder()->build();
    }

    public function cache() : Cache
    {
        return $this->cache;
    }

    public function entryFactory() : EntryFactory
    {
        return $this->entryFactory;
    }

    public function externalSort() : ExternalSort
    {
        return $this->externalSort;
    }

    public function filesystemStreams() : FilesystemStreams
    {
        return $this->filesystemStreams;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function optimizer() : Optimizer
    {
        return $this->optimizer;
    }

    public function shouldPutInputIntoRows() : bool
    {
        return $this->putInputIntoRows;
    }
}
