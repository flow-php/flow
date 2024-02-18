<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;

final class ConfigBuilder
{
    private ?Cache $cache;

    private ?ExternalSort $externalSort;

    private ?Filesystem $filesystem;

    private ?string $id;

    private ?Optimizer $optimizer;

    private bool $putInputIntoRows;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->serializer = null;
        $this->cache = null;
        $this->externalSort = null;
        $this->filesystem = null;
        $this->putInputIntoRows = false;
        $this->optimizer = null;
    }

    /**
     * @psalm-suppress  PossiblyFalseArgument
     */
    public function build() : Config
    {
        $this->id ??= \uniqid('flow_php', true);
        $entryFactory = new NativeEntryFactory();
        $this->serializer ??= new CompressingSerializer(new NativePHPSerializer());
        $cachePath = \is_string(\getenv(Config::CACHE_DIR_ENV)) && \realpath(\getenv(Config::CACHE_DIR_ENV))
            ? \getenv(Config::CACHE_DIR_ENV)
            : \sys_get_temp_dir() . '/flow_php/cache';

        if ($this->cache === null) {
            if (!\file_exists($cachePath)) {
                \mkdir($cachePath, 0777, true);
            }

            $this->cache = new LocalFilesystemCache($cachePath, $this->serializer);
        }

        $this->externalSort ??= new MemorySort(
            $this->id,
            $this->cache,
            \is_string(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) ? Unit::fromString(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) : Unit::fromMb(200)
        );

        // We need to keep it as a string in order to avoid circular dependency between etl and flysystem adapter
        $flysystemFSClass = '\Flow\ETL\Adapter\Filesystem\FlysystemFS';

        if (!$this->filesystem instanceof Filesystem) {
            if (\class_exists($flysystemFSClass)) {
                /** @var Filesystem $flysystemFS */
                $flysystemFS = new $flysystemFSClass();
                $this->filesystem = $flysystemFS;
            } else {
                $this->filesystem = new LocalFilesystem();
            }
        }

        $this->optimizer ??= new Optimizer(
            new Optimizer\LimitOptimization(),
            new Optimizer\BatchSizeOptimization(batchSize: 1000)
        );

        return new Config(
            $this->id,
            $this->serializer,
            $this->cache,
            $this->externalSort,
            new FilesystemStreams($this->filesystem),
            $this->optimizer,
            $this->putInputIntoRows,
            $entryFactory
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

    public function optimizer(Optimizer $optimizer) : self
    {
        $this->optimizer = $optimizer;

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
