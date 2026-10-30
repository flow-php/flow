<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\S3Client;

final readonly class LiveS3Client implements S3ClientSource
{
    public function __construct(
        private S3Client $client,
    ) {}

    public function client(): S3Client
    {
        return $this->client;
    }

    public function normalize(): ?array
    {
        return null;
    }
}
