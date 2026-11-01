<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/host/
 */
enum HostAttribute: string
{
    /**
     * The CPU architecture the host system is running on.
     *
     * @see HostArchitecture
     */
    case ARCH = 'host.arch';

    /**
     * Unique host ID.
     */
    case ID = 'host.id';

    /**
     * Available IP addresses of the host.
     */
    case IP = 'host.ip';

    /**
     * Available MAC addresses of the host.
     */
    case MAC = 'host.mac';

    /**
     * Name of the host.
     */
    case NAME = 'host.name';

    /**
     * Type of host.
     */
    case TYPE = 'host.type';
}
