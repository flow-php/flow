<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Config\Sort\SortConfig;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\{FilesystemTable};
use Flow\Serializer\Serializer;
use Psr\Clock\ClockInterface;

/**
 * Immutable configuration that can be used to initialize many contexts.
 * Configuration must not be changed after it's passed to FlowContext.
 */
final readonly class Config
{
    /**
     * @deprecated Use CacheConfig::CACHE_DIR_ENV instead
     */
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    /**
     * @deprecated Use SortConfig::SORT_MAX_MEMORY_ENV instead
     */
    public const SORT_MAX_MEMORY_ENV = 'FLOW_SORT_MAX_MEMORY';

    public function __construct(
        private string $id,
        private Serializer $serializer,
        private ClockInterface $clock,
        private FilesystemTable $filesystemTable,
        private FilesystemStreams $filesystemStreams,
        private Optimizer $optimizer,
        private Caster $caster,
        private bool $putInputIntoRows,
        private EntryFactory $entryFactory,
        public CacheConfig $cache,
        public SortConfig $sort,
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

    public function caster() : Caster
    {
        return $this->caster;
    }

    public function clock() : ClockInterface
    {
        return $this->clock;
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
}
