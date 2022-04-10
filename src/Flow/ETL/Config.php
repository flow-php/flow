<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializer;

final class Config
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public const EXTERNAL_SORT_MAX_MEMORY_ENV = 'FLOW_EXTERNAL_SORT_MAX_MEMORY';

    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly ExternalSort $externalSort,
        private readonly Pipeline $pipeline,
        private readonly Serializer $serializer
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

    public function externalSort() : ExternalSort
    {
        return $this->externalSort;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function pipeline() : Pipeline
    {
        return $this->pipeline;
    }

    public function serializer() : Serializer
    {
        return $this->serializer;
    }
}
