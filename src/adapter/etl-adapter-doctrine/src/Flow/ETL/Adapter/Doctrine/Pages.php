<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final class Pages
{
    public function __construct(readonly public int $total, readonly public int $pageSize)
    {
    }

    public function pages() : int
    {
        return (int) \ceil($this->total / $this->pageSize);
    }
}
