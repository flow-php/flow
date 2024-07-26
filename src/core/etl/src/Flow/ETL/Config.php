<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\{FilesystemTable};
use Flow\Serializer\Serializer;

/**
 * Immutable configuration that can be used to initialize many contexts.
 * Configuration must not be changed after it's passed to FlowContext.
 */
final class Config
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public const SORT_MAX_MEMORY_ENV = 'FLOW_SORT_MAX_MEMORY';

    /**
     * @param int<1, max> $cacheBatchSize
     */
    public function __construct(
        private readonly string $id,
        private readonly Serializer $serializer,
        private readonly Cache $cache,
        private readonly Unit $sortMemoryLimit,
        private readonly FilesystemTable $filesystemTable,
        private readonly FilesystemStreams $filesystemStreams,
        private readonly Optimizer $optimizer,
        private readonly Caster $caster,
        private readonly bool $putInputIntoRows,
        private readonly EntryFactory $entryFactory,
        private readonly int $cacheBatchSize,
    ) {
        if ($this->cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }
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

    /**
     * @return int<1, max>
     */
    public function cacheBatchSize() : int
    {
        return $this->cacheBatchSize;
    }

    public function caster() : Caster
    {
        return $this->caster;
    }

    public function entryFactory() : EntryFactory
    {
        return $this->entryFactory;
    }

    public function filesystemStreams() : FilesystemStreams
    {
        return $this->filesystemStreams;
    }

    public function fstab() : FilesystemTable
    {
        return $this->filesystemTable;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function optimizer() : Optimizer
    {
        return $this->optimizer;
    }

    public function serializer() : Serializer
    {
        return $this->serializer;
    }

    public function shouldPutInputIntoRows() : bool
    {
        return $this->putInputIntoRows;
    }

    public function sortMemoryLimit() : Unit
    {
        return $this->sortMemoryLimit;
    }
}
