<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource;

use Flow\Telemetry\Resource;

/**
 * Return an empty Resource when nothing is detected.
 */
interface ResourceDetector
{
    /**
     * Detect resource attributes from the environment.
     */
    public function detect(): Resource;
}
