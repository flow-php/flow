<?php

declare(strict_types=1);

namespace Flow\Filesystem;

interface DestinationStream extends Stream
{
    public function append(string $data) : self;

    /**
     * @param resource $resource
     */
    public function fromResource($resource) : self;
}
