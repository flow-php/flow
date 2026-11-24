<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Lock;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Migrations\Exception\MigrationException;

use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\Types\DSL\type_integer;
use function hash;
use function unpack;

final readonly class AdvisoryMigrationLock implements MigrationLock
{
    public function __construct(
        private Client $client,
    ) {}

    public function acquire(string $name): bool
    {
        return $this->client->fetchScalarBool(select(func('pg_try_advisory_lock', [param(1)])), [$this->key($name)]);
    }

    public function release(string $name): void
    {
        if (!$this->client->fetchScalarBool(select(func('pg_advisory_unlock', [param(1)])), [$this->key($name)])) {
            throw MigrationException::advisoryLockNotHeld($name);
        }
    }

    private function key(string $name): int
    {
        return type_integer()->assert(unpack('J', hash('xxh64', $name, true))[1]);
    }
}
