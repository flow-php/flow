<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/os/
 */
enum OsAttribute: string
{
    /**
     * Unique identifier for a particular build of the operating system.
     */
    case BUILD_ID = 'os.build_id';

    /**
     * Human-readable description of the OS.
     */
    case DESCRIPTION = 'os.description';

    /**
     * Human-readable name of the OS.
     */
    case NAME = 'os.name';

    /**
     * Operating system type.
     *
     * @see OsType
     */
    case TYPE = 'os.type';

    /**
     * Version string of the operating system.
     */
    case VERSION = 'os.version';
}
