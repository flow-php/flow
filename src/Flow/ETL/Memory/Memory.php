<?php

declare(strict_types=1);

namespace Flow\ETL\Memory;

interface Memory
{
    /**
     * @phpstan-ignore-next-line
     */
    public function save(array $data) : void;
}
