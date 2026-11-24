<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Lock;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Lock\MigrationLock;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\LockInterface;

use function array_key_exists;

final class SymfonyMigrationLock implements MigrationLock
{
    /**
     * @var array<string, LockInterface>
     */
    private array $locks = [];

    public function __construct(
        private readonly LockFactory $factory,
        private readonly float $ttlSeconds = 3600.0,
    ) {}

    public function acquire(string $name): bool
    {
        $lock = $this->factory->createLock($name, $this->ttlSeconds);

        if (!$lock->acquire()) {
            return false;
        }

        $this->locks[$name] = $lock;

        return true;
    }

    public function release(string $name): void
    {
        if (!array_key_exists($name, $this->locks)) {
            throw MigrationException::lockExpired($name);
        }

        $lock = $this->locks[$name];
        unset($this->locks[$name]);

        if (!$lock->isAcquired()) {
            throw MigrationException::lockExpired($name);
        }

        $lock->release();
    }
}
