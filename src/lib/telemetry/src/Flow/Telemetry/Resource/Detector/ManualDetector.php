<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Detector that returns manually specified resource attributes.
 *
 * Use this detector when you need to specify resource attributes explicitly
 * rather than detecting them automatically. This is useful for:
 * - Setting service.name and service.version explicitly
 * - Adding custom attributes not covered by other detectors
 * - Overriding auto-detected values when used with ChainDetector
 *
 * Example usage:
 * ```php
 * $detector = new ManualDetector([
 *     'service.name' => 'my-app',
 *     'service.version' => '1.0.0',
 *     'deployment.environment.name' => 'production',
 * ]);
 *
 * // Combined with auto-detection (manual takes precedence when last):
 * $detector = new ChainDetector(
 *     new OsDetector(),
 *     new HostDetector(),
 *     new ManualDetector(['service.name' => 'my-app']),
 * );
 * ```
 *
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final readonly class ManualDetector implements ResourceDetector
{
    /**
     * @param TAttributeValueMap $attributes
     */
    public function __construct(
        private array $attributes = [],
    ) {}

    public function detect(): Resource
    {
        return Resource::create($this->attributes);
    }
}
