<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\VersionGenerator;

use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\{Version, VersionGenerator};

final readonly class SequentialVersionGenerator implements VersionGenerator
{
    public function __construct(
        private MigrationStore $store,
        private int $padding = 5,
    ) {
    }

    public function generate() : Version
    {
        $latest = $this->store->executedMigrations()->latest();
        $next = $latest === null ? 1 : ((int) (string) $latest->version) + 1;

        return Version::fromString(\str_pad((string) $next, $this->padding, '0', \STR_PAD_LEFT));
    }
}
