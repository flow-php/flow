<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Process attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the Process resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/process/
 */
enum ProcessAttribute : string
{
    /**
     * The command used to launch the process.
     *
     * This is typically the script name or program name.
     *
     * Example: "cmd/otelcol"
     */
    case COMMAND = 'process.command';

    /**
     * All the command arguments (including the command/executable) as received.
     *
     * Example: ["cmd/otelcol", "--config=config.yaml"]
     */
    case COMMAND_ARGS = 'process.command_args';

    /**
     * The full command line as a single string.
     *
     * Example: "php artisan serve"
     */
    case COMMAND_LINE = 'process.command_line';

    /**
     * The name of the process executable.
     *
     * Example: "otelcol"
     */
    case EXECUTABLE_NAME = 'process.executable.name';

    /**
     * The full path to the process executable.
     *
     * Example: "/usr/bin/php"
     */
    case EXECUTABLE_PATH = 'process.executable.path';

    /**
     * The username of the user that owns the process.
     *
     * Example: "root"
     */
    case OWNER = 'process.owner';

    /**
     * Parent process ID (PID).
     *
     * Example: 111
     */
    case PARENT_PID = 'process.parent_pid';

    /**
     * Process ID (PID).
     *
     * Example: 1234
     */
    case PID = 'process.pid';

    /**
     * An additional description about the runtime.
     *
     * Example: "Eclipse OpenJ9 Eclipse OpenJ9 VM openj9-0.21.0"
     */
    case RUNTIME_DESCRIPTION = 'process.runtime.description';

    /**
     * The name of the runtime of this process.
     *
     * Example: "PHP", "OpenJDK Runtime Environment"
     */
    case RUNTIME_NAME = 'process.runtime.name';

    /**
     * The version of the runtime.
     *
     * Example: "8.3.0", "14.0.2"
     */
    case RUNTIME_VERSION = 'process.runtime.version';
}
