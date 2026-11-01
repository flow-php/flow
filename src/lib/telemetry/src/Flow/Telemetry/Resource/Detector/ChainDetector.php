<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Later detectors override earlier keys.
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
