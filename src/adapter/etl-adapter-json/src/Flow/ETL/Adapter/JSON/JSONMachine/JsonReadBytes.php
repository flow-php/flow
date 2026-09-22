<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

final class JsonReadBytes
{
    /**
     * @var int<0, max>
     */
    private int $total = 0;

    /**
     * @param int<0, max> $bytes
     */
    public function add(int $bytes): void
    {
        $this->total += $bytes;
    }

    /**
     * @return int<0, max>
     */
    public function total(): int
    {
        return $this->total;
    }
}
