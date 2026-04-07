<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;

final readonly class VersionResolver
{
    public function __construct(
        private MigrationRepository $repository,
        private MigrationStore $store,
    ) {
    }

    public function resolve(VersionAlias|string $alias) : Version
    {
        $versionAlias = $alias instanceof VersionAlias ? $alias : VersionAlias::tryFrom($alias);

        if ($versionAlias === null) {
            return Version::fromString($alias);
        }

        return match ($versionAlias) {
            VersionAlias::FIRST => $this->resolveFirst(),
            VersionAlias::LATEST => $this->resolveLatest(),
            VersionAlias::PREV => $this->resolvePrev(),
            VersionAlias::NEXT => $this->resolveNext(),
        };
    }

    private function resolveFirst() : Version
    {
        $first = $this->repository->all()->first();

        if ($first === null) {
            throw MigrationException::versionNotFound(Version::fromString('first'));
        }

        return $first->version;
    }

    private function resolveLatest() : Version
    {
        $last = $this->repository->all()->last();

        if ($last === null) {
            throw MigrationException::versionNotFound(Version::fromString('latest'));
        }

        return $last->version;
    }

    private function resolveNext() : Version
    {
        $latest = $this->store->executedMigrations()->latest();
        $available = $this->repository->all();

        if ($latest === null) {
            $first = $available->first();

            if ($first === null) {
                throw MigrationException::versionNotFound(Version::fromString('next'));
            }

            return $first->version;
        }

        $next = $available->after($latest->version)->first();

        if ($next === null) {
            throw MigrationException::versionNotFound(Version::fromString('next'));
        }

        return $next->version;
    }

    private function resolvePrev() : Version
    {
        $latest = $this->store->executedMigrations()->latest();

        if ($latest === null) {
            throw MigrationException::versionNotFound(Version::fromString('prev'));
        }

        $items = \iterator_to_array($this->repository->all()->upTo($latest->version));

        if (\count($items) < 2) {
            return Version::fromString('0');
        }

        return $items[\count($items) - 2]->version;
    }
}
