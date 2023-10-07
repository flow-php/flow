<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\DropDuplicates;

final class Hashes
{
    /**
     * @var array<string, bool>
     */
    private array $hashes = [];

    public function __construct()
    {
    }

    public function add(string $hash) : void
    {
        $this->hashes[$hash] = true;
    }

    public function exists(string $hash) : bool
    {
        return isset($this->hashes[$hash]);
    }
}
