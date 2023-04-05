<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\DropDuplicates;

final class Hashes
{
    private array $hashes = [];

    public function __construct()
    {
    }

    public function add(string $hash) : void
    {
        $this->hashes[] = $hash;
    }

    public function exists(string $hash) : bool
    {
        return \in_array($hash, $this->hashes, true);
    }
}
