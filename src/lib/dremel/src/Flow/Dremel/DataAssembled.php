<?php

declare(strict_types=1);

namespace Flow\Dremel;

final class DataAssembled
{
    /**
     * @param array<mixed> $rows
     * @param DataShredded $shredded
     */
    public function __construct(
        public readonly array $rows,
        public readonly DataShredded $shredded,
    ) {

    }

    public function size() : int
    {
        return \count($this->rows);
    }
}
