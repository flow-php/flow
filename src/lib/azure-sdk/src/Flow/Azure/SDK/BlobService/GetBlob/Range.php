<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlob;

final class Range
{
    private ?int $end;

    private int $start;

    public function __construct(int $start, ?int $end = null)
    {
        $this->start = $start;
        $this->end = $end;
    }

    public function toString() : string
    {
        if (!isset($this->end)) {
            return \sprintf('bytes=%d-', $this->start);
        }

        return \sprintf('bytes=%d-%d', $this->start, $this->end);
    }
}
