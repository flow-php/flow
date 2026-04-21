<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidArgumentException;

final readonly class Mount
{
    public string $protocol;

    public function __construct(string $protocol)
    {
        if (!\preg_match('/^[a-zA-Z][a-zA-Z0-9+.-]+$/', $protocol)) {
            throw new InvalidArgumentException("Invalid mount protocol: '{$protocol}'. Only alphanumeric characters, dots, hyphens and plus signs are allowed.");
        }

        $this->protocol = $protocol;
    }

    public function supports(Path|string $path) : bool
    {
        return $this->protocol === ($path instanceof Path ? $path->protocol() : $path);
    }
}
