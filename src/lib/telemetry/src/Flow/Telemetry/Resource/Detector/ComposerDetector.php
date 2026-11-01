<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Composer\InstalledVersions;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use Flow\Telemetry\Resource\ResourceDetector;
use OutOfBoundsException;

use function class_exists;
use function explode;

/**
 * service.name is the root package name without the vendor prefix.
 */
final readonly class ComposerDetector implements ResourceDetector
{
    public function detect(): Resource
    {
        if (!class_exists(InstalledVersions::class)) {
            return Resource::empty();
        }

        $attributes = [];

        $packageName = InstalledVersions::getRootPackage()['name'];

        $serviceName = $this->extractServiceName($packageName);
        $attributes[ServiceAttribute::NAME->value] = $serviceName;

        $version = $this->detectVersion($packageName);

        if ($version !== null) {
            $attributes[ServiceAttribute::VERSION->value] = $version;
        }

        return Resource::create($attributes);
    }

    private function detectVersion(string $packageName): ?string
    {
        try {
            $version = InstalledVersions::getPrettyVersion($packageName);

            if ($version !== null && $version !== '') {
                return $version;
            }

            $version = InstalledVersions::getVersion($packageName);

            if ($version !== null && $version !== '') {
                return $version;
            }

            return null;
        } catch (OutOfBoundsException) {
            return null;
        }
    }

    private function extractServiceName(string $packageName): string
    {
        $parts = explode('/', $packageName, 2);

        return $parts[1] ?? $packageName;
    }
}
