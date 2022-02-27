<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\Serializer\Serializer;

final class Config
{
    public const CACHE_DIR_ENV = 'FLOW_LOCAL_FILESYSTEM_CACHE_DIR';

    public const EXTERNAL_SORT_MAX_MEMORY_ENV = 'FLOW_EXTERNAL_SORT_MAX_MEMORY';

    private string $id;

    private Cache $cache;

    private ExternalSort $externalSort;

    private Pipeline $pipeline;

    private Serializer $serializer;

    public function __construct(
        string $id,
        Cache $cache,
        ExternalSort $externalSort,
        Pipeline $pipeline,
        Serializer $serializer
    ) {
        $this->id = $id;
        $this->cache = $cache;
        $this->externalSort = $externalSort;
        $this->pipeline = $pipeline;
        $this->serializer = $serializer;
    }

    public static function builder() : ConfigBuilder
    {
        return new ConfigBuilder();
    }

    public static function default() : self
    {
        return self::builder()->build();
    }

    public function id() : string
    {
        return $this->id;
    }

    public function cache() : Cache
    {
        return $this->cache;
    }

    public function externalSort() : ExternalSort
    {
        return $this->externalSort;
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
