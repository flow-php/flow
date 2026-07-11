<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * VCS (Version Control System) attribute keys following OpenTelemetry semantic conventions.
 *
 * These attribute names correspond to the VCS resource attributes as defined
 * in the OpenTelemetry semantic conventions specification.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/vcs/
 */
enum VcsAttribute: string
{
    /**
     * The name of the reference that the HEAD points to.
     *
     * For a branch this is the branch name, for a tag the tag name.
     *
     * Example: "main", "v1.0.0"
     */
    case REF_HEAD_NAME = 'vcs.ref.head.name';

    /**
     * The revision (commit SHA) that the HEAD currently points to.
     *
     * Example: "9d59409acf479dfa0df1c3dad539b4825a8d2bcf"
     */
    case REF_HEAD_REVISION = 'vcs.ref.head.revision';

    /**
     * The type of the reference that the HEAD points to.
     *
     * Example: "branch", "tag"
     */
    case REF_HEAD_TYPE = 'vcs.ref.head.type';

    /**
     * The full URL of the repository providing the remote.
     *
     * Example: "https://github.com/flow-php/flow.git"
     */
    case REPOSITORY_URL = 'vcs.repository.url.full';
}
