<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\ResourceDetector;

use function dirname;
use function file_get_contents;
use function file_put_contents;
use function is_dir;
use function is_file;
use function is_readable;
use function mkdir;
use function serialize;
use function sys_get_temp_dir;
use function unserialize;

/**
 * The file cache never expires: wrap only runtime-stable detection.
 */
final readonly class CachingDetector implements ResourceDetector
{
    private string $cachePath;

    public function __construct(
        private ResourceDetector $detector,
        ?string $cachePath = null,
    ) {
        $this->cachePath = $cachePath ?? sys_get_temp_dir() . '/flow_telemetry_resource.cache';
    }

    public function detect(): Resource
    {
        if (is_file($this->cachePath) && is_readable($this->cachePath)) {
            $cached = $this->loadFromCache();

            if ($cached !== null) {
                return $cached;
            }
        }

        $resource = $this->detector->detect();
        $this->saveToCache($resource);

        return $resource;
    }

    private function loadFromCache(): ?Resource
    {
        $contents = @file_get_contents($this->cachePath);

        if ($contents === false) {
            return null;
        }

        return self::asResource(@unserialize($contents, ['allowed_classes' => [Resource::class, Attributes::class]]));
    }

    private static function asResource(mixed $value): ?Resource
    {
        return $value instanceof Resource ? $value : null;
    }

    private function saveToCache(Resource $resource): void
    {
        $directory = dirname($this->cachePath);

        if (!is_dir($directory)) {
            @mkdir($directory, 0777, true);
        }

        @file_put_contents($this->cachePath, serialize($resource));
    }
}
