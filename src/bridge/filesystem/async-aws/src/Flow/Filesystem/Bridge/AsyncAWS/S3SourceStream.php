<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\{Path, SourceStream};

final class S3SourceStream implements SourceStream
{
    public function __construct(private readonly Path $path, private readonly S3Client $s3Client)
    {
    }

    public function close() : void
    {
        // TODO: Implement close() method.
    }

    public function content() : string
    {
        return $this->s3Client->getObject([
            'Bucket' => $this->path->rootDirectoryName(),
            'Key' => $this->path->skipDirectories(1)?->path(),
        ])->getBody()->getContentAsString();
    }

    public function isOpen() : bool
    {
        // TODO: Implement isOpen() method.
    }

    public function iterate(int $length = 1) : \Generator
    {
        // TODO: Implement iterate() method.
    }

    public function path() : Path
    {
        // TODO: Implement path() method.
    }

    public function read(int $length, int $offset) : string
    {
        // TODO: Implement read() method.
    }

    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        // TODO: Implement readLines() method.
    }

    public function size() : ?int
    {
        // TODO: Implement size() method.
    }
}
