<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/resource/container/
 */
enum ContainerAttribute: string
{
    /**
     * The command used to run the container.
     */
    case COMMAND = 'container.command';

    /**
     * All the command arguments (including the command/executable) as received.
     */
    case COMMAND_ARGS = 'container.command_args';

    /**
     * The full command run by the container.
     */
    case COMMAND_LINE = 'container.command_line';

    /**
     * Container ID.
     */
    case ID = 'container.id';

    /**
     * Container image ID.
     */
    case IMAGE_ID = 'container.image.id';

    /**
     * Name of the image the container was built on.
     */
    case IMAGE_NAME = 'container.image.name';

    /**
     * Container image tag.
     */
    case IMAGE_TAG = 'container.image.tag';

    /**
     * Container name used by container runtime.
     */
    case NAME = 'container.name';

    /**
     * The container runtime managing this container.
     */
    case RUNTIME = 'container.runtime';
}
