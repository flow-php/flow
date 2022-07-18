<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Serializer\Serializer;

final class Config
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public const EXTERNAL_SORT_MAX_MEMORY_ENV = 'FLOW_EXTERNAL_SORT_MAX_MEMORY';

    private ?int $limit = null;

    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly ExternalSort $externalSort,
        private readonly Serializer $serializer,
        private readonly Filesystem $filesystem,
        private ErrorHandler $errorHandler
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

    public function clearLimit() : self
    {
        $this->limit = null;

        return $this;
    }

    public function errorHandler() : ErrorHandler
    {
        return $this->errorHandler;
    }

    public function externalSort() : ExternalSort
    {
        return $this->externalSort;
    }

    public function filesystem() : Filesystem
    {
        return $this->filesystem;
    }

    public function hasLimit() : bool
    {
        return $this->limit !== null;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function limit() : ?int
    {
        return $this->limit;
    }

    public function serializer() : Serializer
    {
        return $this->serializer;
    }

    public function setErrorHandler(ErrorHandler $errorHandler) : self
    {
        $this->errorHandler = $errorHandler;

        return $this;
    }

    public function setLimit(int $limit) : self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException("Limit can't be lower or equal zero, given: {$limit}");
        }

        $this->limit = $limit;

        return $this;
    }
}
