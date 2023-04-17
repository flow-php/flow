<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

interface ExpandResults
{
    public function expand() : bool;
}
