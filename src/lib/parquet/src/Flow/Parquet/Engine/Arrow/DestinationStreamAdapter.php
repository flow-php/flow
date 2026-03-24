<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Arrow\OutputStream;
use Flow\Filesystem\DestinationStream;

final readonly class DestinationStreamAdapter implements OutputStream
{
    public function __construct(private DestinationStream $stream)
    {
    }

    public function append(string $data) : self
    {
        $this->stream->append($data);

        return $this;
    }
}
