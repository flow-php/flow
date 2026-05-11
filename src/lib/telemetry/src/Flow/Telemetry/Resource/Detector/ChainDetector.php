<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Combines multiple resource detectors into a chain.
 *
 * Detectors are executed in order and their results are merged.
 * Later detectors take precedence over earlier ones when there are
 * conflicting attribute keys (allowing for override patterns).
 *
 * This is useful for combining automatic detection with manual overrides:
 * ```php
 * $detector = new ChainDetector(
 *     new OsDetector(),           // Base OS detection
 *     new HostDetector(),         // Host information
 *     new ProcessDetector(),      // Process information
 *     new ComposerDetector(),     // Service from composer
 *     new EnvironmentDetector(),  // Environment overrides (highest precedence)
 * );
 * ```
 *
 * The recommended order is:
 * 1. Infrastructure detectors (OS, Host)
 * 2. Runtime detectors (Process)
 * 3. Application detectors (Composer)
 * 4. Environment overrides (EnvironmentDetector - should be last)
 */
final readonly class ChainDetector implements ResourceDetector
{
    /**
     * @var array<ResourceDetector>
     */
    private array $detectors;

    public function __construct(ResourceDetector ...$detectors)
    {
        $this->detectors = $detectors;
    }

    public function detect(): Resource
    {
        $resource = Resource::empty();

        foreach ($this->detectors as $detector) {
            $detected = $detector->detect();
            $resource = $resource->merge($detected);
        }

        return $resource;
    }
}
