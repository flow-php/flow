<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Store;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\ExecutedMigration;
use Flow\PostgreSql\Migrations\Version;

/**
 * @implements \IteratorAggregate<int, ExecutedMigration>
 */
final readonly class ExecutedMigrations implements \Countable, \IteratorAggregate
{
    /**
     * @var list<ExecutedMigration>
     */
    private array $migrations;

    public function __construct(ExecutedMigration ...$migrations)
    {
        $sorted = \array_values($migrations);

        \usort($sorted, static fn(ExecutedMigration $a, ExecutedMigration $b): int => (
            $a->version->isAfter($b->version) ? 1 : ($b->version->isAfter($a->version) ? -1 : 0)
        ));

        $this->migrations = $sorted;
    }

    public function count(): int
    {
        return \count($this->migrations);
    }

    public function get(Version $version): ExecutedMigration
    {
        foreach ($this->migrations as $migration) {
            if ($migration->version->equals($version)) {
                return $migration;
            }
        }

        throw MigrationException::versionNotFound($version);
    }

    /**
     * @return \ArrayIterator<int, ExecutedMigration>
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->migrations);
    }

    public function has(Version $version): bool
    {
        foreach ($this->migrations as $migration) {
            if ($migration->version->equals($version)) {
                return true;
            }
        }

        return false;
    }

    public function isEmpty(): bool
    {
        return $this->migrations === [];
    }

    public function latest(): ?ExecutedMigration
    {
        if ($this->migrations === []) {
            return null;
        }

        return $this->migrations[\count($this->migrations) - 1];
    }
}
