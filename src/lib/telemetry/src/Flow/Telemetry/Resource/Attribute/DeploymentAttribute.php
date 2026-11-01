<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/
 */
enum DeploymentAttribute: string
{
    /**
     * Name of the deployment environment.
     */
    case ENVIRONMENT_NAME = 'deployment.environment.name';
}
