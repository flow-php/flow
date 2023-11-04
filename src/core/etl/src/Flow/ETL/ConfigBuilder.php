<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\FlysystemFS;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Pipeline\Execution\Processor\FilesystemProcessor;
use Flow\ETL\Pipeline\Execution\Processors;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;

final class ConfigBuilder
{
    private ?Cache $cache;

    private ?ExternalSort $externalSort;

    private ?Filesystem $filesystem;

    private ?string $id;

    private bool $putInputIntoRows;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->cache = null;
        $this->externalSort = null;
        $this->serializer = null;
        $this->filesystem = null;
        $this->putInputIntoRows = false;
    }

    /**
     * @psalm-suppress  PossiblyFalseArgument
     */
    public function build() : Config
    {
        $this->id ??= \uniqid('flow_php', true);
        $this->serializer ??= new CompressingSerializer();
        $this->cache ??= new LocalFilesystemCache(
            \is_string(\getenv(Config::CACHE_DIR_ENV)) && \realpath(\getenv(Config::CACHE_DIR_ENV))
                ? \getenv(Config::CACHE_DIR_ENV)
                : \sys_get_temp_dir(),
            $this->serializer
        );
        $this->externalSort ??= new MemorySort(
            $this->id,
            $this->cache,
            \is_string(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) ? Unit::fromString(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) : Unit::fromMb(200)
        );
        $this->filesystem ??= new FlysystemFS();

        return new Config(
            $this->id,
            $this->cache,
            $this->externalSort,
            $this->serializer,
            new FilesystemStreams($this->filesystem),
            new Processors(
                new FilesystemProcessor()
            ),
            new Optimizer(
                new Optimizer\LimitOptimization()
            ),
            $this->putInputIntoRows,
            new NativeEntryFactory()
        );
    }

    public function cache(Cache $cache) : self
    {
        $this->cache = $cache;

        return $this;
    }

    public function dontPutInputIntoRows() : self
    {
        $this->putInputIntoRows = false;

        return $this;
    }

    public function externalSort(ExternalSort $externalSort) : self
    {
        $this->externalSort = $externalSort;

        return $this;
    }

    public function filesystem(Filesystem $filesystem) : self
    {
        $this->filesystem = $filesystem;

        return $this;
    }

    public function id(string $id) : self
    {
        $this->id = $id;

        return $this;
    }

    /**
     * When set, each extractor will try to put additional rows with input parameters, like for example uri to the source file from which
     * data is extracted.
     */
    public function putInputIntoRows() : self
    {
        $this->putInputIntoRows = true;

        return $this;
    }

    public function reset() : self
    {
        return new self();
    }

    public function serializer(Serializer $serializer) : self
    {
        $this->serializer = $serializer;

        return $this;
    }
}
