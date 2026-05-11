<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Operating system attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the OS resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/os/
 */
enum OsAttribute: string
{
    /**
     * Unique identifier for a particular build of the operating system.
     *
     * Example: "TQ3C.230805.001.B2", "20E247", "22621"
     */
    case BUILD_ID = 'os.build_id';

    /**
     * Human-readable description of the OS.
     *
     * Example: "Microsoft Windows 10 Enterprise", "Ubuntu 20.04.5 LTS"
     */
    case DESCRIPTION = 'os.description';

    /**
     * Human-readable name of the OS.
     *
     * Example: "iOS", "macOS Sonoma", "Ubuntu"
     */
    case NAME = 'os.name';

    /**
     * Operating system type.
     *
     * @see OsType
     *
     * Example: "linux", "windows", "darwin"
     */
    case TYPE = 'os.type';

    /**
     * Version string of the operating system.
     *
     * Example: "14.2.1", "10.0.22621"
     */
    case VERSION = 'os.version';
}
