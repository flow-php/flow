<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/process/
 */
enum ProcessAttribute: string
{
    /**
     * The command used to launch the process.
     */
    case COMMAND = 'process.command';

    /**
     * All the command arguments (including the command/executable) as received.
     */
    case COMMAND_ARGS = 'process.command_args';

    /**
     * The full command line as a single string.
     */
    case COMMAND_LINE = 'process.command_line';

    /**
     * The name of the process executable.
     */
    case EXECUTABLE_NAME = 'process.executable.name';

    /**
     * The full path to the process executable.
     */
    case EXECUTABLE_PATH = 'process.executable.path';

    /**
     * The username of the user that owns the process.
     */
    case OWNER = 'process.owner';

    /**
     * Parent process ID (PID).
     */
    case PARENT_PID = 'process.parent_pid';

    /**
     * Process ID (PID).
     */
    case PID = 'process.pid';

    /**
     * An additional description about the runtime.
     */
    case RUNTIME_DESCRIPTION = 'process.runtime.description';

    /**
     * The name of the runtime of this process.
     */
    case RUNTIME_NAME = 'process.runtime.name';

    /**
     * The version of the runtime.
     */
    case RUNTIME_VERSION = 'process.runtime.version';
}
