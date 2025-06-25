<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

final readonly class Blob
{
    /**
     * @param array<array-key, mixed> $data
     */
    public function __construct(private array $data)
    {
    }

    public function name() : string
    {
        $name = $this->data['Name'] ?? null;

        if (!\is_string($name)) {
            throw new \InvalidArgumentException('Blob name must be a string');
        }

        return $name;
    }

    public function size() : int
    {
        $properties = $this->data['Properties'] ?? null;

        if (!\is_array($properties)) {
            throw new \InvalidArgumentException('Blob properties must be an array');
        }

        $contentLength = $properties['Content-Length'] ?? null;

        if (!\is_string($contentLength) && !\is_int($contentLength)) {
            throw new \InvalidArgumentException('Content-Length must be a string or integer');
        }

        return (int) $contentLength;
    }
}
