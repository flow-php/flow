<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{memory: Memory}>
 */
final class MemoryLoader implements Loader
{
    private Memory $memory;

    public function __construct(Memory $memory)
    {
        $this->memory = $memory;
    }

    public function __serialize() : array
    {
        return [
            'memory' => $this->memory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->memory = $data['memory'];
    }

    public function load(Rows $rows) : void
    {
        $this->memory->save($rows->toArray());
    }
}
