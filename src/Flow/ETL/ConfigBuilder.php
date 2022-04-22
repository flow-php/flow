<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\Serializer;

final class ConfigBuilder
{
    private ?Cache $cache;

    private ?ErrorHandler $errorHandler;

    private ?ExternalSort $externalSort;

    private ?string $id;

    private ?Serializer $serializer;

    public function __construct()
    {
        $this->id = null;
        $this->cache = null;
        $this->externalSort = null;
        $this->serializer = null;
        $this->errorHandler = null;
    }

    /**
     * @psalm-suppress  PossiblyFalseArgument
     */
    public function build() : Config
    {
        $this->id ??= \uniqid('flow_php');
        $this->serializer ??= new CompressingSerializer();
        $this->cache ??= new LocalFilesystemCache(
            \is_string(\getenv(Config::CACHE_DIR_ENV))
                ? \getenv(Config::CACHE_DIR_ENV)
                : \sys_get_temp_dir(),
            $this->serializer
        );
        $this->externalSort ??= new MemorySort(
            $this->id,
            $this->cache,
            \is_string(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) ? Unit::fromString(\getenv(Config::EXTERNAL_SORT_MAX_MEMORY_ENV)) : Unit::fromMb(200)
        );
        $this->errorHandler ??= new ThrowError();

        return new Config(
            $this->id,
            $this->cache,
            $this->externalSort,
            $this->serializer,
            $this->errorHandler
        );
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

    public function id(string $id) : self
    {
        $this->id = $id;

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
