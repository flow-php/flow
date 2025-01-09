<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlob;

final readonly class Range
{
    public function __construct(private int $start, private ?int $end = null)
    {
    }

    public function toString() : string
    {
        if (!isset($this->end)) {
            return \sprintf('bytes=%d-', $this->start);
        }

        return \sprintf('bytes=%d-%d', $this->start, $this->end);
    }
}
