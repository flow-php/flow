<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config\Cache;

use Flow\ETL\Runtime\CacheConfig;
use Flow\ETL\Runtime\Telemetry\TelemetryConfig;
use Flow\ETL\Runtime\Telemetry\TraceableRowsStorage;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function getenv;
use function is_string;
use function sys_get_temp_dir;

final class CacheConfigBuilder
{
    private ?RowsStorage $cache = null;

    private ?Path $cacheDir = null;

    public function build(
        Backend $backend,
        ?TelemetryConfig $telemetryConfig = null,
        string $dataframeName = 'flow_dataframe',
    ): CacheConfig {
        if ($this->cacheDir !== null) {
            $cachePath = $this->cacheDir;
        } else {
            $envCacheDir = getenv(CacheConfig::CACHE_DIR_ENV) ?: '';
            $cachePath = Path::realpath($envCacheDir !== '' ? $envCacheDir : sys_get_temp_dir() . '/flow_php/cache');
        }

        $cache = $this->cache ?? new FilesystemRowsStorage(new NativeLocalFilesystem(), $cachePath, $backend);

        if ($telemetryConfig !== null && $telemetryConfig->options->traceCache) {
            $cache = new TraceableRowsStorage($cache, $telemetryConfig->telemetry, $dataframeName);
        }

        return new CacheConfig(cache: $cache, localFilesystemCacheDir: $cachePath);
    }

    public function cache(RowsStorage $cache): self
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
        $this->cacheDir = is_string($dir) ? Path::from($dir) : $dir;

        return $this;
    }
}
