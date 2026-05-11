<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Migrations\VersionGenerator;

final class FixedVersionGenerator implements VersionGenerator
{
    public function __construct(
        public Version $version,
    ) {}

    public function generate(): Version
    {
        return $this->version;
    }
}
