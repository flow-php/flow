<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;

final class ConfigBuilder
{
    private ?string $id;

    private ?Cache $cache;

    private ?ExternalSort $externalSort;

    private ?Pipeline $pipeline;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->cache = null;
        $this->externalSort = null;
        $this->pipeline = null;
        $this->serializer = null;
    }

    public function reset() : self
    {
        return new self();
    }

    public function id(string $id) : self
    {
        $this->id = $id;

        return $this;
    }

    public function cache(Cache $cache) : self
    {
        $this->cache = $cache;

        return $this;
    }

    public function externalSort(ExternalSort $externalSort) : self
    {
        $this->externalSort = $externalSort;

        return $this;
    }

    public function pipeline(Pipeline $pipeline) : self
    {
        $this->pipeline = $pipeline;

        return $this;
    }

    public function serializer(Serializer $serializer) : self
    {
        $this->serializer = $serializer;

        return $this;
    }

    /**
     * @psalm-suppress  PossiblyFalseArgument
     */
    public function build() : Config
    {
        $this->id = $this->id ?? \uniqid('flow_php');
        $this->serializer = $this->serializer ?? new CompressingSerializer(new NativePHPSerializer());
        $this->cache = $this->cache ?? new LocalFilesystemCache(
            \is_string(\getenv(Config::CACHE_DIR_ENV))
                ? \getenv(Config::CACHE_DIR_ENV)
                : \sys_get_temp_dir(),
            $this->serializer
        );
        $this->externalSort = $this->externalSort ??
            new MemorySort(
                $this->id,
                $this->cache,
                \is_string(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) ? Unit::fromString(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) : Unit::fromMb(200)
            );
        $this->pipeline = $this->pipeline ?? new SynchronousPipeline();

        return new Config(
            $this->id,
            $this->cache,
            $this->externalSort,
            $this->pipeline,
            $this->serializer
        );
    }
}
