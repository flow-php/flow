<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint\UniqueConstraint;

interface Storage
{
    public function clear() : void;

    public function delete(string $key) : void;

    public function has(string $key) : bool;

    public function set(string $key) : void;
}
