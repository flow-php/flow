<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

final readonly class Blob
{
    public function __construct(private array $data)
    {
    }

    public function name() : string
    {
        return $this->data['Name'];
    }

    public function size() : int
    {
        return (int) $this->data['Properties']['Content-Length'];
    }
}
