<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Cache\Implementation\TraceableCache;
use Flow\ETL\Config\Telemetry\TelemetryConfig;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Serializer\Serializer;

use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function getenv;
use function is_string;
use function sys_get_temp_dir;

final class CacheConfigBuilder
{
    private ?Cache $cache = null;

    private ?Path $cacheDir = null;

    public function build(
        Serializer $serializer,
        ?TelemetryConfig $telemetryConfig = null,
        string $dataframeName = 'flow_dataframe',
    ): CacheConfig {
        if ($this->cacheDir !== null) {
            $cachePath = $this->cacheDir;
        } else {
            $envCacheDir = getenv(CacheConfig::CACHE_DIR_ENV) ?: '';
            $cachePath = path_real($envCacheDir !== '' ? $envCacheDir : sys_get_temp_dir() . '/flow_php/cache');
        }

        $cache = $this->cache ?? new FilesystemCache(
            new NativeLocalFilesystem(),
            cacheDir: $cachePath,
            serializer: $serializer,
        );

        if ($telemetryConfig !== null && $telemetryConfig->options->traceCache) {
            $cache = new TraceableCache($cache, $telemetryConfig->telemetry, $dataframeName);
        }

        return new CacheConfig(cache: $cache, localFilesystemCacheDir: $cachePath);
    }

    public function cache(Cache $cache): self
    {
        $this->cache = $cache;

        return $this;
    }

    /**
     * Sets the local filesystem cache directory explicitly, overriding the FLOW_LOCAL_FILESYSTEM_CACHE_DIR
     * env var and the system temp fallback.
     */
    public function cacheDir(string|Path $dir): self
    {
        $this->cacheDir = is_string($dir) ? path($dir) : $dir;

        return $this;
    }
}
