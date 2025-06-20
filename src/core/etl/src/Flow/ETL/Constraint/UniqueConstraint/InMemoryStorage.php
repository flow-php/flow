<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint\UniqueConstraint;

final class InMemoryStorage implements Storage
{
    /** @var array<string, bool> */
    private array $storage = [];

    public function __construct()
    {
    }

    public function clear() : void
    {
        $this->storage = [];
    }

    public function delete(string $key) : void
    {
        if ($this->has($key)) {
            unset($this->storage[$key]);
        }
    }

    public function has(string $key) : bool
    {
        return isset($this->storage[$key]);
    }

    public function set(string $key) : void
    {
        $this->storage[$key] = true;
    }
}
