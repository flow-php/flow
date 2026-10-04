<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function count;

final class Buffers
{
    private int $position = 0;

    /**
     * @param list<string> $buffers
     */
    public function __construct(
        private readonly array $buffers,
    ) {}

    public function next(): string
    {
        if ($this->position >= count($this->buffers)) {
            throw new InvalidArgumentException('Column buffers exhausted: the column needs more buffers than given');
        }

        return $this->buffers[$this->position++];
    }

    public function remaining(): int
    {
        return count($this->buffers) - $this->position;
    }
}
