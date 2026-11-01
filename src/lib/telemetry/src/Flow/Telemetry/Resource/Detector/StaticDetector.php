<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Detector;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * @import-type TAttributeValueMap from Attributes
 */
final readonly class StaticDetector implements ResourceDetector
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
