<?php

declare(strict_types=1);

namespace Flow\Telemetry\Resource\Attribute;

/**
 * @see https://opentelemetry.io/docs/specs/semconv/registry/attributes/vcs/
 */
enum VcsAttribute: string
{
    /**
     * The name of the reference that the HEAD points to.
     */
    case REF_HEAD_NAME = 'vcs.ref.head.name';

    /**
     * The revision (commit SHA) that the HEAD currently points to.
     */
    case REF_HEAD_REVISION = 'vcs.ref.head.revision';

    /**
     * The type of the reference that the HEAD points to.
     */
    case REF_HEAD_TYPE = 'vcs.ref.head.type';

    /**
     * The full URL of the repository providing the remote.
     */
    case REPOSITORY_URL = 'vcs.repository.url.full';
}
