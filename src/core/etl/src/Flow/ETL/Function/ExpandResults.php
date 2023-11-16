<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

interface ExpandResults
{
    public function expand() : bool;
}
