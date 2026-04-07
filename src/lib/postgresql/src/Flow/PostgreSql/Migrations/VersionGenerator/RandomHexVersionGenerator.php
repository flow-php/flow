<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\VersionGenerator;

use Flow\PostgreSql\Migrations\{Version, VersionGenerator};

final readonly class RandomHexVersionGenerator implements VersionGenerator
{
    public function __construct(
        private int $length = 12,
    ) {
    }

    public function generate() : Version
    {
        return Version::fromString(\bin2hex(\random_bytes(\max(1, (int) \ceil($this->length / 2)))));
    }
}
