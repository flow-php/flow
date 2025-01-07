<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class Pages
{
    public function __construct(public int $total, public int $pageSize)
    {
    }

    public function pages() : int
    {
        return (int) \ceil($this->total / $this->pageSize);
    }
}
