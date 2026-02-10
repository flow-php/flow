<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Common deployment environment values.
 *
 * While OpenTelemetry defines `deployment.environment.name` as a string,
 * this enum provides common standardized values for convenience.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/
 */
enum DeploymentEnvironment : string
{
    /**
     * Development environment for active development work.
     */
    case DEVELOPMENT = 'development';

    /**
     * Local environment for developer machines.
     */
    case LOCAL = 'local';

    /**
     * Production environment serving live traffic.
     */
    case PRODUCTION = 'production';

    /**
     * Staging/pre-production environment.
     */
    case STAGING = 'staging';

    /**
     * Testing environment for automated tests.
     */
    case TESTING = 'testing';

    /**
     * Get the deployment environment attribute key.
     *
     * Returns the standard OpenTelemetry attribute key for deployment environment.
     */
    public static function attributeKey() : string
    {
        return 'deployment.environment.name';
    }
}
