<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobServiceInterface;

/**
 * @import-type BlobServiceConfiguration from ConfiguredBlobService
 */
interface BlobServiceSource
{
    public function blobService(): BlobServiceInterface;

    /**
     * @return null|BlobServiceConfiguration null for a live service that cannot be rebuilt elsewhere
     */
    public function normalize(): ?array;
}
