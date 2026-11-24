<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Lock\MigrationLock;
use Throwable;

use function array_shift;

final class SpyMigrationLock implements MigrationLock
{
    /**
     * @var list<bool>
     */
    public array $acquireResults = [];

    public bool $acquired = true;

    /**
     * @var list<array{string, string}>
     */
    public array $calls = [];

    public ?Throwable $releaseFailure = null;

    public function acquire(string $name): bool
    {
        $this->calls[] = ['acquire', $name];

        return array_shift($this->acquireResults) ?? $this->acquired;
    }

    public function release(string $name): void
    {
        $this->calls[] = ['release', $name];

        if ($this->releaseFailure !== null) {
            throw $this->releaseFailure;
        }
    }
}
