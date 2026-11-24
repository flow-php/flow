<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double;

use Symfony\Component\Lock\Key;
use Symfony\Component\Lock\PersistingStoreInterface;

final class ForgetfulLockStore implements PersistingStoreInterface
{
    public bool $exists = true;

    public function delete(Key $key): void {}

    public function exists(Key $key): bool
    {
        return $this->exists;
    }

    public function putOffExpiration(Key $key, float $ttl): void {}

    public function save(Key $key): void {}
}
