<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Resource\Detector;

use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Attribute\DeploymentAttribute;
use Flow\Telemetry\Resource\ResourceDetector;

/**
 * Detects deployment environment from Symfony kernel environment.
 *
 * Sets deployment.environment.name attribute to the Symfony kernel environment value.
 */
final readonly class SymfonyDeploymentDetector implements ResourceDetector
{
    public function __construct(
        private string $kernelEnvironment,
    ) {
    }

    public function detect() : Resource
    {
        return Resource::create([
            DeploymentAttribute::ENVIRONMENT_NAME->value => $this->kernelEnvironment,
        ]);
    }
}
