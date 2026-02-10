<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * Container attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the Container resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/resource/container/
 */
enum ContainerAttribute : string
{
    /**
     * The command used to run the container.
     *
     * Example: "otelcontribcol"
     */
    case COMMAND = 'container.command';

    /**
     * All the command arguments (including the command/executable) as received.
     *
     * Example: ["otelcontribcol", "--config", "/etc/otel-config.yaml"]
     */
    case COMMAND_ARGS = 'container.command_args';

    /**
     * The full command run by the container.
     *
     * Example: "otelcontribcol --config /etc/otel-config.yaml"
     */
    case COMMAND_LINE = 'container.command_line';

    /**
     * Container ID.
     *
     * Usually a UUID, but this can be implementation-specific.
     *
     * Example: "a3bf90e006b2"
     */
    case ID = 'container.id';

    /**
     * Container image ID.
     *
     * Often a sha256 hash.
     *
     * Example: "sha256:19c92d0a00d1b66d897bceaa7319bee0dd38a10a851c60bcec9474aa3f01e50f"
     */
    case IMAGE_ID = 'container.image.id';

    /**
     * Name of the image the container was built on.
     *
     * Example: "gcr.io/opentelemetry/operator"
     */
    case IMAGE_NAME = 'container.image.name';

    /**
     * Container image tag.
     *
     * Example: "0.1"
     */
    case IMAGE_TAG = 'container.image.tag';

    /**
     * Container name used by container runtime.
     *
     * Example: "opentelemetry-autoconf"
     */
    case NAME = 'container.name';

    /**
     * The container runtime managing this container.
     *
     * Example: "docker", "containerd", "podman"
     */
    case RUNTIME = 'container.runtime';
}
