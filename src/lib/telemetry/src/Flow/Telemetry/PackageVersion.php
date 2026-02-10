<?php

declare(strict_types=1);

namespace Flow\Telemetry;

use Composer\InstalledVersions;

final class PackageVersion
{
    public static function get(string $packageName) : string
    {
        if (InstalledVersions::isInstalled($packageName)) {
            return InstalledVersions::getPrettyVersion($packageName) ?? 'unknown';
        }

        return 'unknown';
    }
}
