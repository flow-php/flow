<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Service attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the Service resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/#service
 */
enum ServiceAttribute: string
{
    /**
     * The string ID of the service instance.
     *
     * Must be unique for each instance of the same service.namespace/service.name pair.
     * A random UUID is recommended.
     *
     * Example: "627cc493-f310-47de-96bd-71410b7dec09"
     */
    case INSTANCE_ID = 'service.instance.id';

    /**
     * Logical name of the service.
     *
     * Must be the same for all instances of horizontally scaled services.
     * Required attribute.
     *
     * Example: "shopping-cart"
     */
    case NAME = 'service.name';

    /**
     * A namespace for service.name.
     *
     * Helps to distinguish a group of services, for example the team name
     * that owns a group of services.
     *
     * Example: "Shop"
     */
    case NAMESPACE = 'service.namespace';

    /**
     * The version string of the service API or implementation.
     *
     * Example: "2.0.0", "1.0.0-beta"
     */
    case VERSION = 'service.version';
}
