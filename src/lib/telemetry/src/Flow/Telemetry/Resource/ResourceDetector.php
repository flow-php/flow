<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource;

use Flow\Telemetry\Resource;

/**
 * Interface for detecting resource attributes from the environment.
 *
 * Resource detectors automatically discover information about the entity
 * producing telemetry (such as OS, host, process, service information).
 * Multiple detectors can be chained together using ChainDetector.
 *
 * Implementations should return a Resource with detected attributes,
 * or an empty Resource if detection fails or produces no data.
 *
 * Example usage:
 * ```php
 * $detector = new ChainDetector(
 *     new OsDetector(),
 *     new HostDetector(),
 *     new ProcessDetector(),
 *     new EnvironmentDetector(),
 * );
 *
 * $resource = $detector->detect();
 * ```
 */
interface ResourceDetector
{
    /**
     * Detect resource attributes from the environment.
     */
    public function detect(): Resource;
}
