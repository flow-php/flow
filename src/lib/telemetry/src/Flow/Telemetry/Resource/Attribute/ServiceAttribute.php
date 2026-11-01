<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/#service
 */
enum ServiceAttribute: string
{
    /**
     * The string ID of the service instance.
     */
    case INSTANCE_ID = 'service.instance.id';

    /**
     * Logical name of the service.
     */
    case NAME = 'service.name';

    /**
     * A namespace for service.name.
     */
    case NAMESPACE = 'service.namespace';

    /**
     * The version string of the service API or implementation.
     */
    case VERSION = 'service.version';
}
