<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Repository;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Version;

/**
 * @implements \IteratorAggregate<int, AvailableMigration>
 */
final readonly class AvailableMigrations implements \Countable, \IteratorAggregate
{
    /**
     * @var list<AvailableMigration>
     */
    private array $migrations;

    public function __construct(AvailableMigration ...$migrations)
    {
        $sorted = \array_values($migrations);

        \usort($sorted, static fn(AvailableMigration $a, AvailableMigration $b): int => (
            $a->version->isAfter($b->version) ? 1 : ($b->version->isAfter($a->version) ? -1 : 0)
        ));

        $this->migrations = $sorted;
    }

    public function after(Version $version): self
    {
        return new self(...\array_filter(
            $this->migrations,
            static fn(AvailableMigration $m): bool => $m->version->isAfter($version),
        ));
    }

    public function count(): int
    {
        return \count($this->migrations);
    }

    public function first(): ?AvailableMigration
    {
        return $this->migrations[0] ?? null;
    }

    public function get(Version $version): AvailableMigration
    {
        foreach ($this->migrations as $migration) {
            if ($migration->version->equals($version)) {
                return $migration;
            }
        }

        throw MigrationException::versionNotFound($version);
    }

    /**
     * @return \ArrayIterator<int, AvailableMigration>
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

    public function last(): ?AvailableMigration
    {
        if ($this->migrations === []) {
            return null;
        }

        return $this->migrations[\count($this->migrations) - 1];
    }

    public function upTo(Version $version): self
    {
        return new self(...\array_filter(
            $this->migrations,
            static fn(AvailableMigration $m): bool => !$m->version->isAfter($version),
        ));
    }
}
