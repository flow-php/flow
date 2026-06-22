<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use DateTimeImmutable;

final readonly class MigrationStatus
{
    public function __construct(
        public Version $version,
        public string $name,
        public MigrationState $state,
        public ?DateTimeImmutable $executedAt,
        public ?int $executionTimeMs = null,
    ) {}
}
