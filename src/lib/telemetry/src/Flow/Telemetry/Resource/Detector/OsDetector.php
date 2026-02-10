<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\{OsAttribute, OsType};
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Detects operating system information.
 *
 * Detects the following attributes:
 * - os.type: The operating system type (linux, windows, darwin, etc.)
 * - os.name: The operating system name
 * - os.version: The operating system version
 * - os.description: Human-readable description of the OS
 *
 * Uses PHP's php_uname() function to gather system information.
 *
 * Example output:
 * ```
 * os.type: darwin
 * os.name: Darwin
 * os.version: 24.0.0
 * os.description: Darwin Kernel Version 24.0.0...
 * ```
 */
final readonly class OsDetector implements ResourceDetector
{
    public function detect() : Resource
    {
        $osType = OsType::detect();

        $attributes = [];

        if ($osType !== null) {
            $attributes[OsAttribute::TYPE->value] = $osType->value;
        }

        $sysname = \php_uname('s');

        if ($sysname !== '') {
            $attributes[OsAttribute::NAME->value] = $sysname;
        }

        $release = \php_uname('r');

        if ($release !== '') {
            $attributes[OsAttribute::VERSION->value] = $release;
        }

        $fullDescription = \php_uname('a');

        if ($fullDescription !== '') {
            $attributes[OsAttribute::DESCRIPTION->value] = $fullDescription;
        }

        return Resource::create($attributes);
    }
}
