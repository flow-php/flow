<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

final readonly class ExecutedMigration
{
    public function __construct(
        public Version $version,
        public \DateTimeImmutable $executedAt,
        public ?int $executionTimeMs,
    ) {
    }
}
