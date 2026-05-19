<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Executor;

use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Version;
use Throwable;

final readonly class ExecutionResult
{
    public function __construct(
        public Version $version,
        public Direction $direction,
        public int $executionTimeMs,
        public bool $skipped,
        public ?Throwable $error,
    ) {}

    public function isSuccessful(): bool
    {
        return $this->error === null && !$this->skipped;
    }
}
