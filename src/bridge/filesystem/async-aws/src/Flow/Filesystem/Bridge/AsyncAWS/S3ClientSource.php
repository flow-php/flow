<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\S3Client;

/**
 * @import-type S3ClientConfiguration from ConfiguredS3Client
 */
interface S3ClientSource
{
    public function client(): S3Client;

    /**
     * @return null|S3ClientConfiguration null for a live client that cannot be rebuilt elsewhere
     */
    public function normalize(): ?array;
}
