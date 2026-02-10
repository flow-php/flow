<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Deployment attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the Deployment resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/
 */
enum DeploymentAttribute : string
{
    /**
     * Name of the deployment environment.
     *
     * Common values: development, staging, production, testing
     *
     * Example: "production"
     */
    case ENVIRONMENT_NAME = 'deployment.environment.name';
}
