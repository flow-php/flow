<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Host attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the Host resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/host/
 */
enum HostAttribute: string
{
    /**
     * The CPU architecture the host system is running on.
     *
     * @see HostArchitecture
     *
     * Example: "amd64", "arm64"
     */
    case ARCH = 'host.arch';

    /**
     * Unique host ID.
     *
     * For cloud resources, this should be the cloud provider's instance ID.
     * For non-cloud, this could be the machine-id from /etc/machine-id (Linux)
     * or a similar system identifier.
     *
     * Example: "i-1234567890abcdef0"
     */
    case ID = 'host.id';

    /**
     * Available IP addresses of the host.
     *
     * Example: ["192.168.1.100", "fd00::1"]
     */
    case IP = 'host.ip';

    /**
     * Available MAC addresses of the host.
     *
     * MAC addresses should be uppercase and colon-separated.
     *
     * Example: ["AA:BB:CC:DD:EE:FF"]
     */
    case MAC = 'host.mac';

    /**
     * Name of the host.
     *
     * This may be a hostname, FQDN, or another host identifier.
     *
     * Example: "web-server-1.example.com"
     */
    case NAME = 'host.name';

    /**
     * Type of host.
     *
     * For cloud resources, this could be the machine type like "t2.medium".
     *
     * Example: "n1-standard-1"
     */
    case TYPE = 'host.type';
}
