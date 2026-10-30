<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobServiceInterface;

final readonly class LiveBlobService implements BlobServiceSource
{
    public function __construct(
        private BlobServiceInterface $blobService,
    ) {}

    public function blobService(): BlobServiceInterface
    {
        return $this->blobService;
    }

    public function normalize(): ?array
    {
        return null;
    }
}
