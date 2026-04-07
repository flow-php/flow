<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\VersionGenerator;

use Flow\PostgreSql\Migrations\{Version, VersionGenerator};

final readonly class TimestampVersionGenerator implements VersionGenerator
{
    public function __construct(
        private string $format = 'YmdHis',
    ) {
    }

    public function generate() : Version
    {
        return Version::fromString(\date($this->format));
    }
}
